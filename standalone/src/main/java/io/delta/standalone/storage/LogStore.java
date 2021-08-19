/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.standalone.storage;

import io.delta.standalone.data.CloseableIterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Iterator;

public abstract class LogStore {

    private Configuration initHadoopConf;

    public LogStore(Configuration initHadoopConf) {
        this.initHadoopConf = initHadoopConf;
    }

    public Configuration initHadoopConf() { return initHadoopConf; }

    public abstract CloseableIterator<String> read(Path path, Configuration hadoopConf);

    public abstract void write(
        Path path,
        Iterator<String> actions,
        Boolean overwrite,
        Configuration hadoopConf) throws FileAlreadyExistsException;

    public abstract Iterator<FileStatus> listFrom(
        Path path,
        Configuration hadoopConf) throws FileNotFoundException;

    public abstract Path resolvePathOnPhysicalStorage(Path path, Configuration hadoopConf);

    public abstract Boolean isPartialWriteVisible(Path path, Configuration hadoopConf);
}
