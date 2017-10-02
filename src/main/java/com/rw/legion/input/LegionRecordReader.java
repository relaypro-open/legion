/*
 * Credit: This file includes code derived from the Apache Software Foundation's
 * Hadoop project (http://hadoop.apache.org/) and released under the Apache
 * License, Version 2.0.
 * 
 * Copyright (C) 2017 Republic Wireless
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rw.legion.input;

import com.rw.legion.LegionObjective;
import com.rw.legion.LegionRecord;
import com.rw.legion.ObjectiveDeserializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;

import java.io.IOException;

/**
 * Abstract <code>RecordReader</code> that produces <code>NullWritable</code>
 * keys and <code>LegionRecord</code> values. Implementations should extend
 * this class and implement the <code>makeRecord</code> method, which parses
 * a line from the file using the appropriate format (e.g., CSV, JSON) and
 * returns a <code>LegionRecord</code>.
 * 
 * Produces one <code>LegionRecord</code> per line in a file.
 * 
 * This is simply a modification of the default Hadoop
 * <code>LineRecordReader</code>.
 */
@InterfaceAudience.LimitedPrivate({"MapReduce", "Pig"})
@InterfaceStability.Evolving
public abstract class LegionRecordReader
        extends RecordReader<NullWritable, LegionRecord> {
    private static final Log LOG = LogFactory.getLog(LegionRecordReader.class);
    public static final String MAX_LINE_LENGTH = 
        "mapreduce.input.linerecordreader.line.maxlength";

    private long start;
    private long pos;
    private long end;
    private SplitLineReader in;
    private FSDataInputStream fileIn;
    private Seekable filePosition;
    private int maxLineLength;
    private LegionRecord value;
    private boolean isCompressedInput;
    private Decompressor decompressor;
    private byte[] recordDelimiterBytes;
    
    protected String fileName;
    private boolean fileBroken;
    protected Text currentLine;
    protected long currentLineNumber;
    private LegionObjective legionObjective;

    public LegionRecordReader() {
    }

    public LegionRecordReader(byte[] recordDelimiter) {
        this.recordDelimiterBytes = recordDelimiter;
    }

    public void initialize(InputSplit genericSplit,
            TaskAttemptContext context) throws IOException {
        /*
         * fileBroken tracks whether there's been an IOException while reading
         * this file. If there has, the record reader will simply stop reading
         * records for this particular file, rather than blowing up the whole
         * job.
         */
        fileBroken = false;
        currentLine = new Text();
        currentLineNumber = 0;
        
        FileSplit split = (FileSplit) genericSplit;
        
        if (split.getLength() == 0) {
            fileBroken = true;
        }
        
        // Load the Legion Objective.
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
        legionObjective =
                ObjectiveDeserializer.deserialize(job.get("legion_objective"));
        
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();

        // Open the file and seek to the start of the split
        final FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(file);
        
        // Grab the file name to include with the data.
        fileName = file.toString();
        
        // Does the Legion Objective specify an input codec to use?
        if (legionObjective.getCodecOverride() != null) {
            isCompressedInput = true;
            CompressionCodec codec = new CompressionCodecFactory(job)
                .getCodecByClassName(legionObjective.getCodecOverride());
            decompressor = CodecPool.getDecompressor(codec);
            in = new SplitLineReader(
                    codec.createInputStream(fileIn, decompressor),
                    job, this.recordDelimiterBytes);
            filePosition = fileIn;
        } else {
            CompressionCodec codec
                = new CompressionCodecFactory(job).getCodec(file);
            if (null!=codec) {
                isCompressedInput = true;    
                decompressor = CodecPool.getDecompressor(codec);
                
                if (codec instanceof SplittableCompressionCodec) {
                    final SplitCompressionInputStream cIn =
                        ((SplittableCompressionCodec)codec).createInputStream(
                            fileIn, decompressor, start, end,
                            SplittableCompressionCodec.READ_MODE.BYBLOCK);
                    in = new CompressedSplitLineReader(cIn, job,
                            this.recordDelimiterBytes);
                    start = cIn.getAdjustedStart();
                    end = cIn.getAdjustedEnd();
                    filePosition = cIn;
                } else {
                    in = new SplitLineReader(
                            codec.createInputStream(fileIn, decompressor),
                            job, this.recordDelimiterBytes);
                    filePosition = fileIn;
                }
            } else {
                fileIn.seek(start);
                in = new SplitLineReader(fileIn, job,
                        this.recordDelimiterBytes);
                filePosition = fileIn;
            }
        }
        
        /*
         * If this is not the first split, we always throw away first record
         * because we always (except the last split) read one extra line in
         * next() method.
         */
        if (start != 0) {
          start += in.readLine(new Text(), 0, maxBytesToConsume(start));
        }
        
        this.pos = start;
    }
    

    private int maxBytesToConsume(long pos) {
        return isCompressedInput
            ? Integer.MAX_VALUE
            : (int) Math.max(Math.min(Integer.MAX_VALUE, end - pos),
                maxLineLength);
    }

    private long getFilePosition() throws IOException {
        long retVal;
        
        if (fileBroken) {
            retVal = end + 1;
        } else if (isCompressedInput && null != filePosition) {
            retVal = filePosition.getPos();
        } else {
            retVal = pos;
        }
        return retVal;
    }

    private int skipUtfByteOrderMark() throws IOException {
        // Strip BOM(Byte Order Mark)
        // Text only support UTF-8, we only need to check UTF-8 BOM
        // (0xEF,0xBB,0xBF) at the start of the text stream.
        int newMaxLineLength = (int) Math.min(3L + (long) maxLineLength,
                Integer.MAX_VALUE);
        int newSize = in.readLine(currentLine, newMaxLineLength,
                maxBytesToConsume(pos));
        // Even we read 3 extra bytes for the first line,
        // we won't alter existing behavior (no backwards incompat issue).
        // Because the newSize is less than maxLineLength and
        // the number of bytes copied to Text is always no more than newSize.
        // If the return size from readLine is not less than maxLineLength,
        // we will discard the current line and read the next line.
        pos += newSize;
        int textLength = currentLine.getLength();
        byte[] textBytes = currentLine.getBytes();
        if ((textLength >= 3) && (textBytes[0] == (byte)0xEF) &&
                (textBytes[1] == (byte)0xBB) && (textBytes[2] == (byte)0xBF)) {
            // find UTF-8 BOM, strip it.
            LOG.info("Found UTF-8 BOM and skipped it");
            textLength -= 3;
            newSize -= 3;
            if (textLength > 0) {
                // It may work to use the same buffer and not do the copyBytes
                textBytes = currentLine.copyBytes();
                currentLine.set(textBytes, 3, textLength);
            } else {
                currentLine.clear();
            }
        }
        return newSize;
    }

    public boolean nextKeyValue() throws IOException {
        int newSize = 0;
        
        // We always read one extra line, which lies outside the upper
        // split limit i.e. (end - 1)
        while (getFilePosition() <= end ||
                in.needAdditionalRecordAfterSplit()) {
            currentLineNumber ++;
            
            try {
                if (pos == 0) {
                    newSize = skipUtfByteOrderMark();
                } else {
                    newSize = in.readLine(currentLine, maxLineLength,
                            maxBytesToConsume(pos));
                    pos += newSize;
                }

                if ((newSize == 0) || (newSize < maxLineLength)) {
                    value = makeRecord();
                    
                    if (value != null) {
                        break;
                    }
                }
    
                // Line too long, or didn't get turned into a record. Try again.
            } catch(IOException e) {
                fileBroken = true;
            }
        }
        
        if (newSize == 0 || fileBroken) {
            value = null;
            return false;
        } else {
            return true;
        }
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public LegionRecord getCurrentValue() {
        return value;
    }

    /**
     * Get the progress within the split.
     */
    public float getProgress() throws IOException {
        if (fileBroken) {
            return 1.0f;
        } else if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (getFilePosition() - start) /
                (float)(end - start));
        }
    }
    
    public synchronized void close() throws IOException {
        try {
            if (in != null) {
                in.close();
            }
        } finally {
            if (decompressor != null) {
                CodecPool.returnDecompressor(decompressor);
            }
        }
    }
    
    protected abstract LegionRecord makeRecord();
}
