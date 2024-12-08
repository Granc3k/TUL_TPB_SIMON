FROM confluentinc/cp-kafka:7.5.0

# Switch to root to install jq
USER root

# Install jq directly (without epel-release)
RUN yum install -y jq && yum clean all

# Create a non-root user for Kafka
RUN useradd -m -s /bin/bash kafkauser

# Change ownership of necessary Kafka directories
RUN chown -R kafkauser /var/lib/kafka /etc/kafka /var/log/kafka

# Switch back to non-root user
USER kafkauser
