/**
 * Copyright (c) 2019. Qubole Inc
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.qubole.rubix.hadoop2;

import com.qubole.rubix.core.CachingFileSystem;
import com.qubole.rubix.spi.ClusterType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

import java.io.IOException;
import java.net.URI;

/**
 * Created by suryansh agnihotri 24 feb 2020
 */
public class CachingAzureBlobFileSystem extends CachingFileSystem<AzureBlobFileSystem>
{
  private static final String SCHEME = "abfs";

  public CachingAzureBlobFileSystem()
  {
    super();
  }

  @Override
  public ClusterType getClusterType() {
    return ClusterType.HADOOP2_CLUSTER_MANAGER;
  }

  public String getScheme()
  {
    return SCHEME;
  }
}
