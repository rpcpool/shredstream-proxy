use std::{
    io, net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket}, num::NonZeroUsize, process::Command
};

use serde::Deserialize;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};

fn run_ip_json(args: &[&str]) -> io::Result<Vec<u8>> {
    let output = Command::new("ip").args(args).output()?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "`ip {}` failed with status {}",
                args.join(" "),
                output.status
            ),
        ))
    }
}

/// Parse multicast groups routed to `device` via `ip --json route show dev <device>`
pub fn get_ip_route_for_device(device: &str) -> io::Result<Vec<IpAddr>> {
    let stdout = run_ip_json(&["--json", "route", "show", "dev", device])?;
    parse_ip_route_for_device(&stdout)
}

// Pure JSON parsers for unit testing
pub fn parse_ip_route_for_device(bytes: &[u8]) -> io::Result<Vec<IpAddr>> {
    #[derive(Debug, Deserialize)]
    struct RouteRow {
        dst: String,
    }

    let mut groups = serde_json::from_slice::<Vec<RouteRow>>(bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        .into_iter()
        .filter_map(|r| {
            if let Some((base, mask_str)) = r.dst.split_once('/') {
                let ip: IpAddr = base.parse().ok()?;
                let mask: u8 = mask_str.parse().ok()?;
                let is_exact = match ip {
                    IpAddr::V4(_) => mask == 32, // check if full-length mask (not partial)
                    IpAddr::V6(_) => mask == 128,
                };
                (ip.is_multicast() && is_exact).then_some(ip)
            } else {
                let ip: IpAddr = r.dst.parse().ok()?;
                ip.is_multicast().then_some(ip)
            }
        })
        .collect::<Vec<_>>();

    groups.sort_unstable();
    groups.dedup();
    Ok(groups)
}

/// Return the primary IPv4 address configured on `device` (if any), via `ip --json addr show`.
pub fn ipv4_addr_for_device(device: &str) -> io::Result<Option<Ipv4Addr>> {
    let stdout = run_ip_json(&["--json", "addr", "show", "dev", device])?;
    parse_ipv4_addr_from_ip_addr_show_json(&stdout)
}

/// Return the interface index for `device` (if any), via `ip --json link show`.
pub fn ifindex_for_device(device: &str) -> io::Result<Option<u32>> {
    let stdout = run_ip_json(&["--json", "link", "show", "dev", device])?;
    parse_ifindex_from_ip_link_show_json(&stdout)
}

// Pure JSON parsers for unit testing
pub fn parse_ipv4_addr_from_ip_addr_show_json(bytes: &[u8]) -> io::Result<Option<Ipv4Addr>> {
    #[derive(Debug, Deserialize)]
    struct AddrInfo {
        family: Option<String>,
        local: Option<String>,
    }
    #[derive(Debug, Deserialize)]
    struct IfaceRow {
        addr_info: Option<Vec<AddrInfo>>,
    }

    let rows: Vec<IfaceRow> =
        serde_json::from_slice(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let ip = rows
        .into_iter()
        .flat_map(|row| row.addr_info.unwrap_or_default())
        .find_map(|info| {
            (info.family.as_deref() == Some("inet"))
                .then_some(info.local)
                .flatten()
        })
        .and_then(|s| s.parse::<Ipv4Addr>().ok());

    Ok(ip)
}

pub fn parse_ifindex_from_ip_link_show_json(bytes: &[u8]) -> io::Result<Option<u32>> {
    #[derive(Debug, Deserialize)]
    struct LinkRow {
        ifindex: Option<u32>,
    }

    let rows: Vec<LinkRow> =
        serde_json::from_slice(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(rows.into_iter().last().and_then(|r| r.ifindex))
}


pub struct TritonMulticastConfigV4 {
    pub multicast_ip: Ipv4Addr,
    pub bind_ifname: Option<String>,
    pub listen_port: u16,
}

pub struct TritonMulticastConfigV6 {
    pub multicast_ip: Ipv6Addr,
    pub device_ifname: String,
    pub listen_port: u16,
}

pub enum TritonMulticastConfig {
    Ipv4(TritonMulticastConfigV4),
    Ipv6(TritonMulticastConfigV6),
}

impl TritonMulticastConfig {
    pub fn ip(&self) -> IpAddr {
        match self {
            TritonMulticastConfig::Ipv4(cfg) => IpAddr::V4(cfg.multicast_ip),
            TritonMulticastConfig::Ipv6(cfg) => IpAddr::V6(cfg.multicast_ip),
        }
    }
}

pub fn create_multicast_sockets_triton_v4(
    config: &TritonMulticastConfigV4,
    num_threads: NonZeroUsize,
) -> io::Result<Vec<UdpSocket>> {
    let device_ip = match config.bind_ifname.as_ref() {
        Some(ifname) => {
            ipv4_addr_for_device(ifname)?.ok_or_else(|| 
                io::Error::new(io::ErrorKind::NotFound, format!("No IPv4 address found for device {ifname}"))
            )?
        },
        None => Ipv4Addr::UNSPECIFIED,
    };
    let port = config.listen_port;
    log::info!("multicast device  {} has ip {}", config.bind_ifname.as_deref().unwrap_or("unspecified"), device_ip);
    // Step 1: Create first socket, port = 0 → random ephemeral port
    let first_socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    first_socket.set_reuse_address(true)?;
    first_socket.set_reuse_port(true)?;
    first_socket.set_nonblocking(true)?;
    first_socket.bind(&SockAddr::from(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port)))?;
    first_socket.join_multicast_v4(&config.multicast_ip, &device_ip)?;
    log::info!("Joined multicast group {} on device IP {}", config.multicast_ip, device_ip);
    let local_port = first_socket.local_addr()?.as_socket().unwrap().port();

    // Step 2: Create N-1 sockets using that same port
    let mut sockets = Vec::with_capacity(num_threads.get());
    sockets.push(first_socket.into());

    for _ in 1..num_threads.get() {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_reuse_address(true)?;
        socket.set_reuse_port(true)?;
        socket.bind(&SockAddr::from(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            local_port,
        )))?;
        socket.set_nonblocking(true)?;
        socket.join_multicast_v4(&config.multicast_ip, &device_ip)?;
        sockets.push(socket.into());
    }

    Ok(sockets)
}

// fn create_multicast_socket_triton_v6(
//     config: &TritonMulticastConfigV6,
//     num_threads: usize,
// ) -> Result<UdpSocket, io::Error> {
//     let TritonMulticastConfigV6 {
//         multicast_ip,
//         device_ifname,
//     } = config;

//     let addrv6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0);
//     let socket = UdpSocket::bind(addrv6)?;
//     let ifindex = ifindex_for_device(device_ifname)?
//         .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("No such device {device_ifname}")))?;
//     socket.join_multicast_v6(multicast_ip, ifindex)?;
//     Ok(socket)
// }


pub fn create_multicast_sockets_triton_v6(
    config: &TritonMulticastConfigV6,
    num_threads: NonZeroUsize,
) -> io::Result<Vec<UdpSocket>> {
    // Get the interface index for the device name (e.g. "eth0")
    let ifindex = ifindex_for_device(&config.device_ifname)?
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("No such device {}", config.device_ifname)))?;

    // Step 1: Bind first socket to port 0 to let kernel choose a random available port
    let first_socket = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))?;
    first_socket.set_only_v6(true)?;            // IPv6-only
    first_socket.set_reuse_address(true)?;
    first_socket.set_reuse_port(true)?;
    first_socket.set_nonblocking(true)?;
    first_socket.bind(&SockAddr::from(SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), config.listen_port)))?;
    first_socket.join_multicast_v6(&config.multicast_ip, ifindex)?;
    let local_port = first_socket.local_addr()?.as_socket().unwrap().port();
    // Step 2: Create N-1 additional sockets on the same port for load balancing
    let mut sockets = Vec::with_capacity(num_threads.get());
    sockets.push(first_socket.into());

    for _ in 1..num_threads.get() {
        let socket = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_only_v6(true)?;
        socket.set_reuse_address(true)?;
        socket.set_reuse_port(true)?;
        socket.bind(&SockAddr::from(SocketAddr::new(
            IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            local_port,
        )))?;
        socket.set_nonblocking(true)?;
        socket.join_multicast_v6(&config.multicast_ip, ifindex)?;
        sockets.push(socket.into());
    }

    Ok(sockets)
}

pub fn create_multicast_sockets_triton(
    config: &TritonMulticastConfig,
    num_threads: NonZeroUsize,
) -> Result<Vec<UdpSocket>, io::Error> {

    match config {
        TritonMulticastConfig::Ipv4(cfg) => {
            create_multicast_sockets_triton_v4(cfg, num_threads)
        }
        TritonMulticastConfig::Ipv6(cfg) => {
            create_multicast_sockets_triton_v6(cfg, num_threads)
        }
    }
}


#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::{
        parse_ifindex_from_ip_link_show_json, parse_ip_route_for_device,
        parse_ipv4_addr_from_ip_addr_show_json,
    };

    #[test]
    fn parse_ip_route_for_device_test() {
        let json = r#"[{"dst":"169.254.2.112/31","protocol":"kernel","scope":"link","prefsrc":"169.254.2.113","flags":[]},{"dst":"233.84.178.2","gateway":"169.254.2.112","protocol":"static","flags":[]}]"#;
        let parsed = parse_ip_route_for_device(json.as_bytes()).unwrap();
        assert_eq!(parsed, vec![Ipv4Addr::new(233, 84, 178, 2)]);
    }

    #[test]
    fn parse_ipv4_addr_from_addr() {
        let json = r#"[
            {"addr_info":[
                {"family":"inet6","local":"fe80::1234"},
                {"family":"inet","local":"192.168.1.10"}
            ]}
        ]"#;
        let parsed = parse_ipv4_addr_from_ip_addr_show_json(json.as_bytes()).unwrap();
        assert_eq!(parsed, Some(Ipv4Addr::new(192, 168, 1, 10)));
    }

    #[test]
    fn parse_ipv4_addr_from_addr_show_malformed() {
        let json = r#"{"not":"an array"}"#;
        let res = parse_ipv4_addr_from_ip_addr_show_json(json.as_bytes());
        assert!(res.is_err());
    }

    #[test]
    fn parse_ifindex_from_link_show_present() {
        let json = r#"[
            {"ifindex":3,"ifname":"lo"}
        ]"#;
        let parsed = parse_ifindex_from_ip_link_show_json(json.as_bytes()).unwrap();
        assert_eq!(parsed, Some(3));
    }

    #[test]
    fn parse_ifindex_from_link_show_empty() {
        let json = r#"[]"#;
        let parsed = parse_ifindex_from_ip_link_show_json(json.as_bytes()).unwrap();
        assert_eq!(parsed, None);
    }

    #[test]
    fn parse_ifindex_from_link_show_malformed() {
        let json = r#"{"ifindex":3}"#;
        let res = parse_ifindex_from_ip_link_show_json(json.as_bytes());
        assert!(res.is_err());
    }
}
