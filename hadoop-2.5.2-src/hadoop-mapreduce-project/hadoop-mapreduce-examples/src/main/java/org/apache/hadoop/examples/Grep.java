/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.examples;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.examples.terasort.TeraSort;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/* Extracts matching regexs from input files and counts them. */
public class Grep extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(Grep.class);

	private Grep() {
	} // singleton

	public int run(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("Grep <inDir> <outDir> <regex> [<group>]");
			ToolRunner.printGenericCommandUsage(System.out);
			return 2;
		}

		Path tempDir = new Path("grep-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

		Configuration conf = getConf();
		conf.setInt("mapreduce.map.memory.mb", 3072);
		conf.setInt("mapreduce.map.cpu.vcores", 2);
		conf.setInt("mapreduce.reduce.memory.mb", 2048);
		conf.setInt("mapreduce.reduce.cpu.vcores", 3);
		conf.set(RegexMapper.PATTERN, args[2]);
		if (args.length == 4)
			conf.set(RegexMapper.GROUP, args[3]);

		Job grepJob = new Job(conf);

		try {

			grepJob.setJobName("grep-search");

			FileInputFormat.setInputPaths(grepJob, args[0]);

			grepJob.setMapperClass(RegexMapper.class);

			grepJob.setCombinerClass(LongSumReducer.class);
			grepJob.setReducerClass(LongSumReducer.class);
			FileSystem fs = FileSystem.get(getConf());
			if (fs.exists(tempDir))
				fs.delete(tempDir, true);
			FileOutputFormat.setOutputPath(grepJob, tempDir);
			grepJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			grepJob.setOutputKeyClass(Text.class);
			grepJob.setOutputValueClass(LongWritable.class);
			grepJob.setNumReduceTasks(5);
			grepJob.waitForCompletion(true);

			Job sortJob = new Job(conf);
			sortJob.setJobName("grep-sort");

			FileInputFormat.setInputPaths(sortJob, tempDir);
			sortJob.setInputFormatClass(SequenceFileInputFormat.class);

			sortJob.setMapperClass(InverseMapper.class);

			Path output = new Path(args[1]);
			if (fs.exists(output))
				fs.delete(output, true);
			FileOutputFormat.setOutputPath(sortJob, output);
			sortJob.setSortComparatorClass( // sort by decreasing freq
					LongWritable.DecreasingComparator.class);
			sortJob.setNumReduceTasks(1); // write a single file
			sortJob.waitForCompletion(true);
		} finally {
			FileSystem.get(conf).delete(tempDir, true);
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		int res = ToolRunner.run(new Configuration(), new Grep(), args);
		long endTime = System.currentTimeMillis();
		LOG.info("Time:" + (endTime - startTime) + "ms");
		System.exit(res);
	}

}
