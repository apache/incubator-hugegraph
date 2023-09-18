package org.apache.hugegraph.example;

import static org.apache.hugegraph.backend.page.PageState.EMPTY_BYTES;

import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgKvStore;
import org.apache.hugegraph.store.HgStoreClient;
import org.apache.hugegraph.store.HgStoreSession;

public class ExampleNew {
    public static void main(String[] args) throws Exception {
        testScanTable("hugegraph", "g+v"); // why should with "g+"?
    }

    private static void testScanTable(String graph, String table) {
        /*
         * Valid table is:
         * g+v
         * g+oe
         * g+ie
         * g+olap
         * g+task
         * g+index
         * g+server
         */
        HgStoreClient storeClient = HgStoreClient.create(
            PDConfig.of("127.0.0.1:8686").setEnableCache(false));
        String storeTemplate = "%s/g";
        String store = String.format(storeTemplate, graph);
        HgStoreSession session = storeClient.openSession(store);

        try (HgKvIterator<HgKvEntry> iterators = session.scanIterator(table,
                                                                      0, 100000000,
                                                                      HgKvStore.SCAN_HASHCODE,
                                                                      EMPTY_BYTES)) {
            int count = 0;
            while (iterators.hasNext()) {
                count++;
                HgKvEntry next = iterators.next();
                System.out.println(new String(next.key()) +
                                   " <====> " +
                                   new String(next.value()));
            }
            System.out.println(count);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
