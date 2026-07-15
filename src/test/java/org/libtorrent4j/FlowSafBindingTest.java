package org.libtorrent4j;

import org.junit.Test;
import org.libtorrent4j.swig.flow_saf_storage;
import org.libtorrent4j.swig.session_params;

public final class FlowSafBindingTest {
    @Test
    public void directorBridgeCanBeInstalledOnSessionParams() {
        session_params params = new session_params();
        RecordingStorageBridge bridge = new RecordingStorageBridge();

        try {
            params.set_flow_saf_disk_io_constructor(bridge);
        } finally {
            params.delete();
            bridge.delete();
        }
    }

    @Test(expected = RuntimeException.class)
    public void nullBridgeIsRejected() {
        session_params params = new session_params();

        try {
            params.set_flow_saf_disk_io_constructor(null);
        } finally {
            params.delete();
        }
    }

    private static final class RecordingStorageBridge extends flow_saf_storage {
        @Override
        public int validate_path(String path) {
            return 0;
        }

        @Override
        public int create_directory(String path) {
            return 0;
        }

        @Override
        public int open_file(String path, int mode) {
            return -1;
        }

        @Override
        public long file_size(String path) {
            return -1;
        }

        @Override
        public int rename(String oldPath, String newPath) {
            return 0;
        }

        @Override
        public int remove(String path, boolean recursive) {
            return 0;
        }
    }
}
