/*
 * Credit: This file includes code derived from the Apache Software Foundation's
 * Hadoop project (http://hadoop.apache.org/) and released under the Apache
 * License, Version 2.0.
 * 
 * Copyright (C) 2016 Republic Wireless
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

package com.rw.legion;

import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.io.compress.*;

/**
 * Produces <code>NullWritable</code> keys and <code>LegionRecord</code> values.
 * There is one <code>LegionRecord</code> per line in a file. Each of those
 * lines can contain either a single CSV record or a single JSON object.
 * 
 * This is simply a modification of the default Hadoop
 * <code>LineRecordReader</code>.
 */
@InterfaceAudience.LimitedPrivate({"MapReduce", "Pig"})
@InterfaceStability.Evolving
public class LegionRecordReader
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
    
    private LegionObjective legionObjective;
    private boolean isCsv;
    private boolean fileBroken;
    private String fileName;
    private String[] header;

    public LegionRecordReader() {
    }

    public LegionRecordReader(byte[] recordDelimiter) {
        this.recordDelimiterBytes = recordDelimiter;
    }

    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException {
        /*
         * fileBroken tracks whether there's been an IOException while reading
         * this file. If there has, the record reader will simply stop reading
         * records for this particular file, rather than blowing up the whole
         * job.
         */
        fileBroken = false;
        
        FileSplit split = (FileSplit) genericSplit;
        
        if (split.getLength() == 0) {
            fileBroken = true;
        }
        
        // Load the Legion Objective.
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
        legionObjective = new LegionObjective(job.get("legion_objective"));
        isCsv = legionObjective.getInputDataType().equals("CSV") ? true : false;
        
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();

        // Open the file and seek to the start of the split.
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
        
        // If this is CSV data, get the first line and store it as the header.
        if (isCsv){ 
            try {
                Text tempHeader = new Text();
                getFirstLine(tempHeader);
                header = tempHeader.toString().split(",");
            } catch (IOException e) {
                fileBroken = true;
            }
        }
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
    
    private int getFirstLine(Text line) throws IOException {
        /*
         * Strip UTF-8 Byte Order Mark (0xEF,0xBB,0xBF) at the start of the text
         * stream, if necessary. Increment the file position appropriately,
         * either way. Read the first line of the file into line. Return the
         * amount of real data read (less the BOM, if there was one).
         */
        int newMaxLineLength = (int) Math.min(3L + (long) maxLineLength,
                Integer.MAX_VALUE);
        int newSize = in.readLine(line, newMaxLineLength,
                maxBytesToConsume(pos));

        pos += newSize;
        int textLength = line.getLength();
        byte[] textBytes = line.getBytes();
        
        if ((textLength >= 3) && (textBytes[0] == (byte)0xEF) &&
                (textBytes[1] == (byte)0xBB) && (textBytes[2] == (byte)0xBF)) {
            // find UTF-8 BOM, strip it.
            LOG.info("Found UTF-8 BOM and skipped it");
            textLength -= 3;
            newSize -=3;

            if (textLength > 0) {
                // It may work to use the same buffer and not do the copyBytes.
                textBytes = line.copyBytes();
                line.set(textBytes, 3, textLength);
            } else {
                line.clear();
            }
        }
        
        return newSize;
    }
    
    public boolean nextKeyValue() throws IOException {
        value = new LegionRecord();
        
        int newSize = 0;
        Text thisLine = new Text();
        
        while (getFilePosition() <= end ||
                in.needAdditionalRecordAfterSplit()) {
            try {
                if (pos == 0) {
                    newSize = getFirstLine(thisLine);
                } else {
                    newSize = in.readLine(thisLine, maxLineLength,
                            maxBytesToConsume(pos));
                    pos += newSize;
                }

                if ((newSize == 0) || (newSize < maxLineLength)) {
                    break;
                }
    
                // Line too long. Try again.
                LOG.info("Skipped line of size " + newSize + " at pos " +
                        (pos - newSize));
            } catch(IOException e) {
                fileBroken = true;
            }
        }
        
        if (newSize == 0 || fileBroken) {
            value = null;
            return false;
        } else {
            if (isCsv) {
                value.setFromCsv(header, thisLine.toString());
            } else {
                value.setFromJson(thisLine.toString());
            }
            
            value.setField("file_name",  fileName);
            value.setField("file_position", new Long(pos - newSize).toString());
            
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
}
