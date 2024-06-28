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
package io.airlift.compressor.lz4;

import io.airlift.compressor.Compressor;

import java.lang.foreign.MemorySegment;
import java.util.Optional;

public sealed interface Lz4Compressor
        extends Compressor
        permits Lz4JavaCompressor, Lz4NativeCompressor
{
    int compress(MemorySegment input, MemorySegment output);

    static Lz4Compressor create()
    {
        if (Lz4NativeCompressor.isEnabled()) {
            return new Lz4NativeCompressor();
        }
        return new Lz4JavaCompressor();
    }

    static boolean isNativeEnabled()
    {
        return Lz4Native.isEnabled();
    }

    static Optional<String> nativeError()
    {
        try {
            Lz4Native.verifyEnabled();
            return Optional.empty();
        }
        catch (IllegalStateException e) {
            if (e.getCause() != null) {
                return Optional.of(e.getCause().getMessage());
            }
            return Optional.of(e.getMessage());
        }
    }
}
