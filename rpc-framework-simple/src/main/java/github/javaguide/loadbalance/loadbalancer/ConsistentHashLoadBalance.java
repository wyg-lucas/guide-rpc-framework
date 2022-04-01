package github.javaguide.loadbalance.loadbalancer;

import github.javaguide.loadbalance.AbstractLoadBalance;
import github.javaguide.remoting.dto.RpcRequest;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * refer to dubbo consistent hash load balance: https://github.com/apache/dubbo/blob/2d9583adf26a2d8bd6fb646243a9fe80a77e65d5/dubbo-cluster/src/main/java/org/apache/dubbo/rpc/cluster/loadbalance/ConsistentHashLoadBalance.java
 *
 * @author RicardoZ
 * @createTime 2020年10月20日 18:15:20
 */
@Slf4j
public class ConsistentHashLoadBalance extends AbstractLoadBalance {

    // 负载均衡策略的执行就是在Invoker列表中选出一个Invoker

    private final ConcurrentHashMap<String, ConsistentHashSelector> selectors = new ConcurrentHashMap<>();

    @Override
    protected String doSelect(List<String> serviceAddresses, RpcRequest rpcRequest) {
        // 生成调用列表hashcode
        int identityHashCode = System.identityHashCode(serviceAddresses);
        // 根据rpcRequest生成RPC服务名字
        String rpcServiceName = rpcRequest.getRpcServiceName();
        // 以调用rpcServiceName名为key,获取一致性hash选择器
        ConsistentHashSelector selector = selectors.get(rpcServiceName);
        // 若不在则创建新的选择器
        if (selector == null || selector.identityHashCode != identityHashCode) {
            // 创建ConsistentHashSelector时会生成所有虚拟结点
            selectors.put(rpcServiceName, new ConsistentHashSelector(serviceAddresses, 160, identityHashCode));
            // 获取选择器
            selector = selectors.get(rpcServiceName);
        }
        // 选择节点
        return selector.select(rpcServiceName + Arrays.stream(rpcRequest.getParameters()));
    }

    @Data
    static class ConsistentHashSelector {
        // 虚拟节点
        private final TreeMap<Long, String> virtualInvokers;
        // hashcode
        private final int identityHashCode;

        ConsistentHashSelector(List<String> invokers, int replicaNumber, int identityHashCode) {
            // 创建TreeMap来保存节点
            this.virtualInvokers = new TreeMap<>();
            // hashcode
            this.identityHashCode = identityHashCode;

            // 创建虚拟结点，均匀分布在哈希环上
            // 对每个invoker生成replicaNumber个虚拟结点，并存放于TreeMap中
            for (String invoker : invokers) {
                for (int i = 0; i < replicaNumber / 4; i++) {
                    // 根据md5算法为每4个结点生成一个消息摘要，摘要长为16字节128位。 md5就是一个长16字节占128位的bit数组
                    //这里的意思就是每个节点扩展为160个虚拟节点，然后将虚拟节点分组 4 个一组，
                    //4个的原因是 md5共16字节 ,这一个组里的每个虚拟节点占用生成的md5数组中的4个字节
                    //正好4*4 所以分为4个一组
                    byte[] digest = md5(invoker + i);
                    // 随后将128位分为4部分，0-31,32-63,64-95,95-128，并生成4个32位数，存于long中，long的高32位都为0 long64位
                    // 并作为虚拟结点的key。
                    for (int h = 0; h < 4; h++) {
                        long m = hash(digest, h);
                        virtualInvokers.put(m, invoker);
                    }
                }
            }
        }

        static byte[] md5(String key) {
            MessageDigest md;
            try {
                md = MessageDigest.getInstance("MD5");
                byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
                md.update(bytes);
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }

            return md.digest();
        }

        // ketama算法
        static long hash(byte[] digest, int idx) {
            return ((long) (digest[3 + idx * 4] & 255) << 24
                    | (long) (digest[2 + idx * 4] & 255) << 16
                    | (long) (digest[1 + idx * 4] & 255) << 8
                    | (long) (digest[idx * 4] & 255)) & 4294967295L;
        }

        public String select(String rpcServiceKey) {
            // 根据这个参数生成消息摘要
            byte[] digest = md5(rpcServiceKey);
            //调用hash(digest, 0)，将消息摘要转换为hashCode，这里仅取0-31位来生成HashCode
            //调用sekectForKey方法选择结点。
            return selectForKey(hash(digest, 0));
        }

        // 根据hashcode选择节点
        public String selectForKey(long hashCode) {
            // 找到一个最小上届的key所对应的结点。
            Map.Entry<Long, String> entry = virtualInvokers.tailMap(hashCode, true).firstEntry();
            // 若不存在，那么选择treeMap中第一个结点
            if (entry == null) {
                // 也就是环状选择
                entry = virtualInvokers.firstEntry();
            }

            return entry.getValue();
        }
    }
}
