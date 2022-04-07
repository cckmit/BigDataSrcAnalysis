


Chunk数据区
			

	class ChunkHeader{
		String measurementID;
		int dataSize;
		TSDataType dataType;
		CompressionType compressionType;
		TSEncoding encodingType;
		int numOfPages;
		int serializedSize;
	}
	
	class PageHeader{
		int uncompressedSize;
		int compressedSize;
		Statistics statistics;
	}
			
			
			
			
			ChunkGroupWriter{
				void write(Tablet tablet);
			}
			
			PageWriter{
				void write(long time, double value)
				
				void write(long[] timestamps, int[] values, int batchSize)
				
			}
	
		TsFile文件的数据结构(逻辑)
		
TsFile 001{
	ChunkGroup1: 设备1数据
	ChunkGroup2: 设备2数据; 
	ChunkGroup:  代表一个数据的数据, 其中包括多个测点(Chunk),每个测点包含多个时间段(Page);
	{
		Chunk01
		Chunk02
		Chunk: { // 代表一个测点的所有(部分时间?)数据,  其中按时间区间再划分为若干Page;
			Page01
			Page02
			Page: { // 存储TimeRange 一段时间某测点的具体数据
				Time列数据
				Value列数据
				
				PageHeader: { 
					int uncompressedSize;
					int compressedSize;
					Statistics statistics;
				}
			}
			
			ChunkHeader: {
				String measurementID; 		//测点名, 一个Chunk只包含1个测点数据; 
				int dataSize;
				TSDataType dataType;
				CompressionType compressionType;
				TSEncoding encodingType;
				int numOfPages;				//包含page数量; 
				int serializedSize;
			}
		}
		
		ChunkGroupHeader: { // v2版本叫 ChunkGruopFooter?
			String deviceID;
		}
		
	}
}

	
Metadata元数据区

	TsFileMetadata{
		BloomFilter bloomFilter;
		MetadataIndexNode metadataIndex; {// MetadataIndexNode: 
			List<MetadataIndexEntry> children;
			private long endOffset;
		}
		long metaOffset;
	}

	TsFileIOWriter {
		TsFileOutput out;
		File file;
		ChunkMetadata currentChunkMetadata;
		List<ChunkMetadata> chunkMetadataList;
		List<ChunkGroupMetadata> chunkGroupMetadataList;
		Map<String, List<TimeseriesMetadata>> deviceTimeseriesMetadataMap;
	}

	ChunkGroupMetadata {
		String device;
		List<ChunkMetadata> chunkMetadataList;
	}
	
	TimeseriesMetadata{
		String measurementId;
		long startOffsetOfChunkMetaDataList;
		byte timeSeriesMetadataType;
		int chunkMetaDataListDataSize;
		TSDataType dataType;
		Statistics<?> statistics;
		IChunkMetadataLoader chunkMetadataLoader;
		long ramSize;
		PublicBAOS chunkMetadataListBuffer;
		ArrayList<ChunkMetadata> chunkMetadataList;
	}

	ChunkMetadata{ //Metadata of one chunk 
		String measurementUid;
		long offsetOfChunkHeader;
		TSDataType tsDataType;
		long version;
		List<TimeRange> deleteIntervalList;
		IChunkLoader chunkLoader;
		Statistics statistics;
		String filePath;
		String tsFilePrefixPath;
	}
	
	
	查设备某测点(某Timeseries)的数据
	
	1. 查设备 
	
	2. 获取 TimeseriesMetadata 
		以d1.s1的key查询其TimeseriesMetadat的位置,加载并反序列化成 TimeseriesMetadata;
		遍历其中的chunkMetadataList:List<ChunkMetadata> , 查看每个Chunk的数据;
	3. 根据ChunkMetadata中的 filePath,
		
	
	
	
			
Tablet 一个设备 多个测点多个时间片,多个测点的值

	发动机 

	deviceId	timestamp		mm1:温度	mm2:转速	mm3:状态
	
	device01	1580650800		89.58		NULL		true
	device01	1580650805		NULL		205			true
	
	
	
	// Tablet 
	
	class Tablet{
		String deviceId;
		long[] timestamps;
		List<MeasurementSchema> schemas;
		Map<String, Integer> measurementIndex;
		Object[] values;// values中下标是每个measurement不同测点的 数组值;
	}



ChunkGroupWriterImpl.write(Tablet tablet){
	List<MeasurementSchema> timeseries = tablet.getSchemas();
	for (int i = 0; i < timeseries.size(); i++) {
		String measurementId = timeseries.get(i).getMeasurementId();
		TSDataType dataType = timeseries.get(i).getType();
		writeByDataType(tablet, measurementId, dataType, i);{//writeByDataType(Tablet tablet, String measurementId, TSDataType dataType, int index)
			int batchSize = tablet.rowSize;
			switch (dataType) {
				case INT32:
					chunkWriters.get(measurementId)
						.write(tablet.timestamps, (int[]) tablet.values[index], batchSize);
				case DOUBLE:
					chunkWriters.get(measurementId)
						// 接口方法: write(long[] timestamps, double[] values) : timestamps和values长度相同,对应该measurement的每个 时间戳的每个值: time -> value;
						.write(tablet.timestamps, (double[]) tablet.values[index], batchSize);{//ChunkWriterImpl.write()
							if (isSdtEncoding) {
								batchSize = sdtEncoder.encode(timestamps, values, batchSize);
							}
							pageWriter.write(timestamps, values, batchSize);{//PageWriter.write()
								for (int i = 0; i < batchSize; i++) {
									timeEncoder.encode(timestamps[i], timeOut);
									valueEncoder.encode(values[i], valueOut);
								}
								statistics.update(timestamps, values, batchSize);
							}
							checkPageSizeAndMayOpenANewPage();
						}
			}
		}
	}
}


ChunkWriterImpl.writeAllPagesOfChunkToTsFile(){
	// start to write this column chunk
    writer.startFlushChunk(pageBuffer.size(),numOfPages);{//TsFileIOWriter.startFlushChunk
		ChunkMetadata currentChunkMetadata =new ChunkMetadata(measurementId, tsDataType, out.getPosition(), statistics);
		ChunkHeader header =new ChunkHeader(measurementId,dataSize, tsDataType,compressionCodecName,encodingType,numOfPages);
		header.serializeTo(out.wrapAsStream());
	}
	
	// write all pages of this column
    writer.writeBytesToStream(pageBuffer);{//TsFileIOWriter.writeBytesToStream()
		bytes.writeTo(out.wrapAsStream());
	}
	
	writer.endCurrentChunk();
}



TsFileWriter.flushAllChunkGroups(){
	for (Map.Entry<String, IChunkGroupWriter> entry : groupWriters.entrySet()) {
		String deviceId = entry.getKey();
		IChunkGroupWriter groupWriter = entry.getValue();
		
	}
}








