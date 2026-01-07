/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector;

import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.nativeaccess.NativeAccess;

import java.io.IOException;
import java.nio.file.Path;

public class PrefetchNIOFSDirectory extends NIOFSDirectory {
    public static final int POSIX_FADV_NORMAL     = 0;
    public static final int POSIX_FADV_RANDOM     = 1;
    public static final int POSIX_FADV_SEQUENTIAL = 2;
    public static final int POSIX_FADV_WILLNEED   = 3;
    public static final int POSIX_FADV_DONTNEED   = 4;
    public static final int POSIX_FADV_NOREUSE    = 5;

    private static NativeAccess NATIVE_ACCESS = NativeAccess.instance();

    public PrefetchNIOFSDirectory(Path path) throws IOException {
        super(path);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        int fd = NATIVE_ACCESS.open(name, 0);
        var in = super.openInput(name, context);
        return new FilterIndexInput("", in) {
            @Override
            public void prefetch(long offset, long length) throws IOException {
                NATIVE_ACCESS.fadvise(fd, offset, length, POSIX_FADV_WILLNEED);
            }

            @Override
            public void close() throws IOException {
                super.close();
                NATIVE_ACCESS.close(fd);
            }
        };
    }
}
