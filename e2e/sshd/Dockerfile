# The "pcap host" of the end-to-end test: an sshd that serves the test capture, so datafeedfs reads it over real ssh without any real machine.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends openssh-server && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /run/sshd && ssh-keygen -A \
    && useradd -m -s /bin/sh e2e && usermod -p '*' e2e && mkdir -p /home/e2e/.ssh && chown e2e: /home/e2e/.ssh && chmod 700 /home/e2e/.ssh \
    && printf 'PasswordAuthentication no\nKbdInteractiveAuthentication no\nPubkeyAuthentication yes\nAllowUsers e2e\nUsePAM no\n' > /etc/ssh/sshd_config.d/e2e.conf
CMD ["/usr/sbin/sshd", "-D", "-e"]
