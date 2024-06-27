/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.compressor.lzo;

import io.airlift.compressor.AbstractTestCompression;
import io.airlift.compressor.Compressor;
import io.airlift.compressor.Decompressor;
import io.airlift.compressor.HadoopCodecCompressor;
import io.airlift.compressor.HadoopCodecDecompressor;
import io.airlift.compressor.HadoopCodecDecompressorByteAtATime;
import io.airlift.compressor.HadoopNative;
import io.airlift.compressor.thirdparty.HadoopLzoCompressor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;

class TestLzopCodecByteAtATime
        extends AbstractTestCompression
{
    static {
        HadoopNative.requireHadoopNative();
    }

    private final CompressionCodec verifyCodec;

    TestLzopCodecByteAtATime()
    {
        com.hadoop.compression.lzo.LzopCodec codec = new com.hadoop.compression.lzo.LzopCodec();
        codec.setConf(new Configuration());
        this.verifyCodec = codec;
    }

    @Override
    protected boolean isMemorySegmentSupported()
    {
        return false;
    }

    @Override
    protected Compressor getCompressor()
    {
        return new HadoopCodecCompressor(new LzopCodec(), new HadoopLzoCompressor());
    }

    @Override
    protected Decompressor getDecompressor()
    {
        return new HadoopCodecDecompressorByteAtATime(new LzopCodec());
    }

    @Override
    protected Compressor getVerifyCompressor()
    {
        return new HadoopCodecCompressor(verifyCodec, new HadoopLzoCompressor());
    }

    @Override
    protected Decompressor getVerifyDecompressor()
    {
        return new HadoopCodecDecompressor(verifyCodec);
    }
}
