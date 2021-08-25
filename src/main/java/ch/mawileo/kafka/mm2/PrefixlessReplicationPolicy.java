package ch.mawileo.kafka.mm2;

import java.util.Map;
import org.apache.kafka.connect.mirror.DefaultReplicationPolicy;
import org.apache.kafka.connect.mirror.MirrorConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 *   This replication policy is supposed to be used for a simple two-site active-passive Kafka DRP setup where mirroring is always enabled
 *   in one direction only and where names of the downstream topics are not prefixed by Mirror Maker 2, so that switching of the active site
 *   1) does not require reconfiguration of the business services (i.e., we do not have to change names of the topics the services use) and
 *   2) does not add one more prefix to the topic name (i.e., we do not have topic names growing each time the switch happens).
 * </p>
 * <p>
 *   This solution seems to work with Mirror Maker version 2.4.0 but it is 'hacky' because it violates spec of the ReplicationPolicy
 *   interface. According to the spec, if <code>upstreamTopic</code> returns null then <code>topicSource</code> should also return null,
 *   but with this implementation it is not the case. We had to make <code>topicSource</code> return alias of the upstream cluster because
 *   otherwise mirroring of topic properties (e.g., "cleanup.policy=compact") did not work.
 * </p>
 */

public class PrefixlessReplicationPolicy extends DefaultReplicationPolicy {

  private static final Logger log = LoggerFactory.getLogger(PrefixlessReplicationPolicy.class);

  public static final String SOURCE_CLUSTER_ALIAS_CONFIG = "source.cluster.alias";

    private String sourceClusterAlias = null;

    @Override
    public void configure(Map<String, ?> props) {
        super.configure(props);
        if (props.containsKey(SOURCE_CLUSTER_ALIAS_CONFIG)) {
            sourceClusterAlias = (String) props.get(SOURCE_CLUSTER_ALIAS_CONFIG);
            log.info("Using source cluster alias `{}`.", sourceClusterAlias);
        }
    }

    /** Unlike DefaultReplicationPolicy, IdentityReplicationPolicy does not include the source
      * cluster alias in the remote topic name. Instead, topic names are unchanged.
      *
      * In the special case of heartbeats, we defer to DefaultReplicationPolicy.
      */
    @Override
    public String formatRemoteTopic(String sourceClusterAlias, String topic) {
        if (looksLikeHeartbeat(topic)) {
            return super.formatRemoteTopic(sourceClusterAlias, topic);
        } else {
            return topic;
        }
    }

    /** Unlike DefaultReplicationPolicy, IdendityReplicationPolicy cannot know the source of
      * a remote topic based on its name alone. If `source.cluster.alias` is provided,
      * `topicSource` will return that.
      *
      * In the special case of heartbeats, we defer to DefaultReplicationPolicy.
      */
    @Override
    public String topicSource(String topic) {
        if (looksLikeHeartbeat(topic)) {
            return super.topicSource(topic);
        } else {
            return sourceClusterAlias;
        }
    }

    /** Since any topic may be a "remote topic", this just returns `topic`.
      *
      * In the special case of heartbeats, we defer to DefaultReplicationPolicy.
      */
    @Override
    public String upstreamTopic(String topic) {
        if (looksLikeHeartbeat(topic)) {
            return super.upstreamTopic(topic);
        } else {
            return topic;
        }
    }

    private boolean looksLikeHeartbeat(String topic) {
        return topic != null && topic.endsWith(MirrorClientConfig.HEARTBEATS_TOPIC);
    }

}
