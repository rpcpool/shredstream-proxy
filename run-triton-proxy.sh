
# send random traffic
sudo nping --udp -p 8002 -c 0 --rate 1 --data-length 1200 127.0.0.1

while true; do
  # Generate 1200 random bytes from /dev/urandom and send them
  head -c 1200 /dev/urandom | socat - UDP:127.0.0.1:8002
  sleep 0.01 # Add a small delay between packets
done


# run triton-proxy
cargo run --bin triton-proxy -- forward-only --src-bind-addr 127.0.0.1 --src-bind-port 8002 --prometheus-bind-addr 127.0.0.1:9999 --dest-ip-ports 127.0.0.1:8989