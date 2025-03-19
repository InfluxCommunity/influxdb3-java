FROM cimg/base:current

RUN wget -O- https://apt.envoyproxy.io/signing.key | sudo gpg --dearmor -o /etc/apt/keyrings/envoy-keyring.gpg
RUN sudo echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/envoy-keyring.gpg] https://apt.envoyproxy.io jammy main" | sudo tee /etc/apt/sources.list.d/envoy.list
RUN sudo apt-get update
RUN sudo apt-get install envoy

COPY ./envoy.yml /etc
EXPOSE 10000

ENTRYPOINT ["/usr/bin/envoy", "-c", "/etc/envoy.yml", "-l", "debug"]