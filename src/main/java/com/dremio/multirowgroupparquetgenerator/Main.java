package com.dremio.multirowgroupparquetgenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class Main {
  private static final int columnCount = 50;
  private static final int columnStringDataLength = 10;
  private static final int totalRows = 100000;
  private static final int blockSize = 32 * 1024;
  private static final int pageSize = 4 * 1024;
  private static final String filePath = "/tmp/output.parquet";
  private static final String datatypes = "string";
  private static final int nullPercent = 10;
  private static int temp = 0;

  public static void main(String[] args) {
    CliArgs cliArgs = new CliArgs(args);

    int columns = cliArgs.switchIntValue("-columns", columnCount);
    int stringDataLength = cliArgs.switchIntValue("-cellsize", columnStringDataLength);
    int rows = cliArgs.switchIntValue("-rows", totalRows);
    int block = cliArgs.switchIntValue("-blocksize", blockSize);
    int page = cliArgs.switchIntValue("-pagesize", pageSize);
    String file = cliArgs.switchValue("-path", filePath);
    String types = cliArgs.switchValue("-types", datatypes);
    int nulls = cliArgs.switchIntValue("-nullpercent", nullPercent);
    if (nulls < 0 || nulls > 100) {
      nulls = nullPercent;
    }
    String[] typeArr = types.split(",");
    if (typeArr.length == 0) {
      typeArr = types.split(datatypes);
    }

    System.out.println("-columns " + columns +
      " -types " + types +
      " -cellsize " + stringDataLength +
      " -rows " +
      +rows + " -blocksize " +
      block + " -pagesize " +
      page + " -path " +
      file);
    Schema schema = getFileSchema(columns, typeArr);
    writeToParquet(schema, columns, typeArr,
      stringDataLength, rows, block, page, nulls, file);
  }

  private static void writeToParquet(Schema schema,
                                     int columns,
                                     String[] typeArr,
                                     int stringDataLength,
                                     int rows,
                                     int block,
                                     int page,
                                     int nullPercentage,
                                     String file) {
    Path path = new Path(file);
    ParquetWriter<GenericData.Record> writer = null;
    try {
      writer = AvroParquetWriter.
        <GenericData.Record>builder(path)
        .withRowGroupSize(block)
        .withPageSize(page)
        .withSchema(schema)
        .withConf(new Configuration())
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withValidation(false)
        .withDictionaryEncoding(false)
        .build();

      for (int row = 0; row < rows; ++row) {
        GenericData.Record record = new GenericData.Record(schema);
        for (int col = 0; col < columns; ++col) {
          String type;
          if (col < typeArr.length) {
            type = typeArr[col];
          } else {
            type = typeArr[typeArr.length - 1];
          }

          switch (type.toUpperCase()) {
            case "INT":
              if (temp % 2 == 0) {
                record.put(col, RandomUtils.nextInt());
              } else {
                record.put(col, null);
              }
              temp++;
              break;
            case "FLOAT":
              if (RandomUtils.nextInt() % 100 >= nullPercentage) {
                record.put(col, RandomUtils.nextFloat());
              } else {
                record.put(col, null);
              }
              break;
            case "DOUBLE":
              if (RandomUtils.nextInt() % 100 >= nullPercentage) {
                record.put(col, RandomUtils.nextDouble());
              } else {
                record.put(col, null);
              }
              break;
            case "BIGINT":
              if (RandomUtils.nextInt() % 100 >= nullPercentage) {
                record.put(col, RandomUtils.nextLong());
              } else {
                record.put(col, null);
              }
              break;
            case "DECIMAL": {
              record.put(col, RandomStringUtils.randomNumeric(4).getBytes());
              break;
            }
            case "DATE":
              record.put(col, (RandomUtils.nextInt() % 2020) * 365);
              break;
            case "TIMESTAMP":
              record.put(col, ((RandomUtils.nextLong() % 2020) - 1970) * 365 * 24 * 60 * 60 * 1000);
              break;
            case "ARRAY":
              final IndexedRecord struct = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  return 1;
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
//                            if (RandomUtils.nextInt() % 3 == 0) {
//                                record.put(col, Arrays.asList());
//                            } else {
              List<IndexedRecord> list = new ArrayList<>();
              for (int i = 0; i < 1000; ++i) {
                list.add(struct);
              }
              record.put(col, list);
//                                record.put(col, Arrays.asList(struct, struct));
//                            }
              break;
            case "NULLABLEARRAY": {
              final IndexedRecord struct_na = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  return 1;
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
              int r = RandomUtils.nextInt() % 3;
              if (r == 0) {
                record.put(col, Collections.emptyList());
              } else if (r == 1) {
                record.put(col, Arrays.asList(struct_na, struct_na));
              } else {
                record.put(col, null);
              }
              break;
            }
            case "ARRAY_NULLABLE": {
              final IndexedRecord struct_a_n = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  if (RandomUtils.nextInt() % 2 == 0) {
                    return null;
                  }
                  return 1;
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
              if (RandomUtils.nextInt() % 3 == 0) {
                record.put(col, Collections.emptyList());
              } else {
                record.put(col, Arrays.asList(struct_a_n, struct_a_n));
              }
              break;
            }
            case "STRUCT":
              record.put(col, new IndexedRecord() {
                @Override
                public void put(int i, Object v) {

                }

                @Override
                public Object get(int i) {
                  return 1;
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              });
              break;
            case "NULLABLESTRUCT":
              record.put(col, RandomUtils.nextInt() % 3 == 0 ? null : new IndexedRecord() {
                @Override
                public void put(int i, Object v) {

                }

                @Override
                public Object get(int i) {
                  return 1;
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              });
              break;
            case "STRUCT_NULLABLE":
              record.put(col, new IndexedRecord() {
                @Override
                public void put(int i, Object v) {

                }

                @Override
                public Object get(int i) {
                  if (RandomUtils.nextInt() % 3 == 0) {
                    return null;
                  }
                  return 1;
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              });
              break;
            case "ARRAY_STRUCT": {
              final IndexedRecord struct1 = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  return 1;
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
              IndexedRecord elem = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  return struct1;
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
              if (RandomUtils.nextInt() % 3 == 0) {
                record.put(col, Collections.emptyList());
              } else {
                record.put(col, Arrays.asList(elem, elem));
              }
              break;
            }

            case "STRUCT_ARRAY":
              final IndexedRecord elem = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  return 1;
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
              IndexedRecord arr = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  if (RandomUtils.nextInt() % 3 == 0) {
                    return Collections.emptyList();
                  }
                  return Arrays.asList(elem, elem);
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
              record.put(col, arr);
              break;
            case "ARRAY_STRUCT_STRUCT":
              final IndexedRecord struct2 = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  return 1;
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
              final IndexedRecord struct1 = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  return struct2;
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
              IndexedRecord elem2 = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  return struct1;
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
              if (RandomUtils.nextInt() % 3 == 0) {
                record.put(col, Collections.emptyList());
              } else {
                record.put(col, Arrays.asList(elem2, elem2));
              }
              break;
            case "ARRAY_STRUCT_NULLSTRUCT_STRUCT":
              final IndexedRecord struct5 = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  return 1;
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
              final IndexedRecord struct3 = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  return struct5;
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
              final IndexedRecord struct4 = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  if (RandomUtils.nextInt() % 3 == 0) {
                    return null;
                  }
                  return struct3;

                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
              IndexedRecord elem3 = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  return struct4;
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
              if (RandomUtils.nextInt() % 2 == 0) {
                record.put(col, Collections.emptyList());
              } else {
                record.put(col, Arrays.asList(elem3, elem3));
              }
              break;
            case "ARRAY_NULLARRAY_ARRAY":
              final IndexedRecord elem6 = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  return 1;
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
              final IndexedRecord elem5 = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  if (RandomUtils.nextInt() % 2 == 0) {
                    return Collections.emptyList();
                  }
                  return Arrays.asList(elem6, elem6);
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
              final IndexedRecord elem4 = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  int r = RandomUtils.nextInt() % 3;
                  if (r == 0) {
                    return null;
                  } else if (r == 1) {
                    return Collections.emptyList();
                  } else {
                    return Arrays.asList(elem5, elem5);
                  }
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
              if (RandomUtils.nextInt() % 2 == 0) {
                record.put(col, Collections.emptyList());
              } else {
                record.put(col, Arrays.asList(elem4, elem4));
              }
              break;
            case "MAP":
              if (RandomUtils.nextInt() % 3 == 0) {
                record.put(col, Collections.emptyMap());
              } else {
                record.put(col, new HashMap<String, Integer>() {
                  {
                    put("k1", 1);
                    put("k2", 2);
                  }
                });
              }
              break;

            case "MAP_NULLABLEVAL":
              int r = RandomUtils.nextInt() % 3;
              if (r == 0) {
                record.put(col, Collections.emptyMap());
              } else if (r == 1) {
                record.put(col, new HashMap<String, Integer>() {
                  {
                    put("k1", 1);
                    put("k2", 2);
                    put("k3", null);
                  }
                });
              } else {
                record.put(col, Collections.singletonMap("k", null));
              }
              break;
            case "ARRAY_MAP":
              final IndexedRecord mapelem = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  if (RandomUtils.nextInt() % 3 == 0) {
                    return Collections.emptyMap();
                  }
                  return new HashMap<String, Integer>() {
                    {
                      put("k1", 1);
                      put("k2", 2);
                    }
                  };
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
              if (RandomUtils.nextInt() % 4 == 0) {
                record.put(col, Collections.emptyList());
              } else {
                record.put(col, Arrays.asList(mapelem, mapelem));
              }
              break;
            case "ARRAY_MAP_NULLABLESTRUCT":
              final IndexedRecord structelem = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  return 1;
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
              final IndexedRecord arraymapelem = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  int r = RandomUtils.nextInt() % 3;
                  if (r == 0) {
                    return Collections.emptyMap();
                  }
                  return new HashMap<String, Object>() {
                    {
                      put("k1", structelem);
                      put("k2", structelem);
                      put("k3", null);
                    }
                  };
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
              if (RandomUtils.nextInt() % 4 == 0) {
                record.put(col, Collections.emptyList());
              } else {
                record.put(col, Arrays.asList(arraymapelem, arraymapelem));
              }
              break;
            case "LIST_MAP_NULLABLEVAL_LIST_MAP_LIST_STRUCT":
              final IndexedRecord mapstr = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  return 1;
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
              final IndexedRecord mapelem3 = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  return mapstr;
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
              final IndexedRecord mapelem2 = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  if (RandomUtils.nextInt() % 3 == 0) {
                    return Collections.emptyMap();
                  }
                  return new HashMap<String, Object>() {
                    {
                      put("k1", Collections.emptyList());
                      put("k2", Arrays.asList(mapelem3, mapelem3));
                    }
                  };
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
              final IndexedRecord mapelem1 = new IndexedRecord() {
                @Override
                public void put(int i, Object v) {
                }

                @Override
                public Object get(int i) {
                  if (RandomUtils.nextInt() % 3 == 0) {
                    return Collections.emptyMap();
                  }
                  return new HashMap<String, Object>() {
                    {
                      put("k0", Collections.emptyList());
                      put("k1", null);
                      put("k2", Arrays.asList(mapelem2, mapelem2));
                      put("k3", Arrays.asList(mapelem2, mapelem2));
                    }
                  };
                }

                @Override
                public Schema getSchema() {
                  return null;
                }
              };
              if (RandomUtils.nextInt() % 6 == 0) {
                record.put(col, Collections.emptyList());
              } else {
                record.put(col, Arrays.asList(mapelem1, mapelem1));
              }
              break;
            case "STRING":
              if (RandomUtils.nextInt() % 100 >= nullPercentage) {
                record.put(col, RandomStringUtils.randomAlphanumeric(stringDataLength));
              } else {
                record.put(col, null);
              }
              break;
            default:
              throw new RuntimeException("");
          }

        }
        writer.write(record);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      IOUtils.closeQuietly(writer);
    }
  }

  private static Schema getFileSchema(int columns, String[] typeArr) {
    SchemaBuilder.RecordBuilder<Schema> schemaRecordBuilder = SchemaBuilder.record("heythere");
    SchemaBuilder.FieldAssembler<Schema> schemaFieldAssembler = schemaRecordBuilder.fields();
    for (int i = 0; i < columns; ++i) {
      String type;
      if (i < typeArr.length) {
        type = typeArr[i];
      } else {
        type = typeArr[typeArr.length - 1];
      }

      switch (type.toUpperCase()) {
        case "INT":
          schemaFieldAssembler = schemaFieldAssembler.optionalInt("f" + i);
          break;
        case "FLOAT":
          schemaFieldAssembler = schemaFieldAssembler.optionalFloat("f" + i);
          break;
        case "DOUBLE":
          schemaFieldAssembler = schemaFieldAssembler.optionalDouble("f" + i);
          break;
        case "BIGINT":
          schemaFieldAssembler = schemaFieldAssembler.optionalLong("f" + i);
          break;
        case "DECIMAL": {
          Schema decimalType = LogicalTypes.decimal(10, 5).addToSchema(Schema.create(Schema.Type.BYTES));
          schemaFieldAssembler = schemaFieldAssembler.name("f" + i).type(decimalType).noDefault();
          break;
        }
        case "DATE": {
          Schema dateType = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
          schemaFieldAssembler = schemaFieldAssembler.name("f" + i).type(dateType).noDefault();
          break;
        }
        case "TIMESTAMP": {
          Schema timestampType = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
          schemaFieldAssembler = schemaFieldAssembler.name("f" + i).type(timestampType).noDefault();
          break;
        }
        case "ARRAY": {
          schemaFieldAssembler = schemaFieldAssembler.name("f" + i).type().array().items().record("test").fields().requiredInt("element").endRecord().noDefault();
          schemaFieldAssembler = schemaFieldAssembler.name("f" + i).type().optional().array().items().record("test").fields().optionalInt("element").endRecord();
          break;
        }
        case "NULLABLEARRAY": {
          schemaFieldAssembler = schemaFieldAssembler.name("f" + i).type().optional().array().items().record("test").fields().requiredInt("element").endRecord();
          break;
        }
        case "ARRAY_NULLABLE": {
          schemaFieldAssembler = schemaFieldAssembler.name("f" + i).type().array().items().record("test").fields().optionalInt("element").endRecord().noDefault();
          break;
        }
        case "STRUCT": {
          schemaFieldAssembler = schemaFieldAssembler.name("f" + i).type().record("test").fields().requiredInt("field").endRecord().noDefault();
          break;
        }
        case "NULLABLESTRUCT": {
          schemaFieldAssembler = schemaFieldAssembler.name("f" + i).type().optional().record("test").fields().requiredInt("field").endRecord();
          break;
        }
        case "STRUCT_NULLABLE": {
          schemaFieldAssembler = schemaFieldAssembler.name("f" + i).type().record("test").fields().optionalInt("field").endRecord().noDefault();
          break;
        }
        case "ARRAY_STRUCT": {
          schemaFieldAssembler = schemaFieldAssembler.name("f" + 1)
            .type()
            .array()
            .items()
            .record("test")
            .fields()
            .name("element")
            .type()
            .record("struct")
            .fields()
            .requiredInt("field")
            .endRecord()
            .noDefault()
            .endRecord()
            .noDefault();
          break;
        }
        case "STRUCT_ARRAY":
          schemaFieldAssembler = schemaFieldAssembler.name("f" + 1)
            .type()
            .record("test")
            .fields()
            .name("field")
            .type()
            .array()
            .items()
            .record("test2")
            .fields()
            .requiredInt("element")
            .endRecord()
            .noDefault()
            .endRecord()
            .noDefault();

          break;
        case "ARRAY_STRUCT_STRUCT":
          schemaFieldAssembler = schemaFieldAssembler.name("f" + 1)
            .type()
            .array()
            .items()
            .record("test")
            .fields()
            .name("element")
            .type()
            .record("struct1")
            .fields()
            .name("element2")
            .type()
            .record("struct2")
            .fields()
            .requiredInt("field")
            .endRecord()
            .noDefault()
            .endRecord()
            .noDefault()
            .endRecord()
            .noDefault();
          break;
        case "ARRAY_STRUCT_NULLSTRUCT_STRUCT":
          schemaFieldAssembler = schemaFieldAssembler.name("f" + 1)
            .type()
            .array()
            .items()
            .record("test")
            .fields()
            .name("element")
            .type()
            .record("struct1")
            .fields()
            .name("element2")
            .type()
            .optional()
            .record("struct2")
            .fields()
            .name("element3")
            .type()
            .record("struct3")
            .fields()
            .requiredInt("field")
            .endRecord()
            .noDefault()
            .endRecord()
            .endRecord()
            .noDefault()
            .endRecord()
            .noDefault();
          break;
        case "ARRAY_NULLARRAY_ARRAY":
          schemaFieldAssembler = schemaFieldAssembler.name("f" + 1)
            .type()
            .array()
            .items()
            .record("test")
            .fields()
            .name("element")
            .type()
            .optional()
            .array()
            .items()
            .record("test2")
            .fields()
            .name("element")
            .type()
            .array()
            .items()
            .record("test4")
            .fields()
            .requiredInt("element")
            .endRecord()
            .noDefault()
            .endRecord()
            .endRecord()
            .noDefault();
          break;
        case "MAP":
          schemaFieldAssembler = schemaFieldAssembler.name("f" + 1)
            .type()
            .map()
            .values(Schema.create(Schema.Type.INT))
            .noDefault();
          break;
        case "MAP_NULLABLEVAL":
          schemaFieldAssembler = schemaFieldAssembler.name("f" + 1)
            .type()
            .map()
            .values()
            .nullable()
            .intType()
            .noDefault();
          break;
        case "ARRAY_MAP":
          schemaFieldAssembler = schemaFieldAssembler.name("f" + 1)
            .type()
            .array()
            .items()
            .record("test")
            .fields()
            .name("element")
            .type()
            .map()
            .values(Schema.create(Schema.Type.INT))
            .noDefault()
            .endRecord()
            .noDefault();
          break;
        case "ARRAY_MAP_NULLABLESTRUCT":
          schemaFieldAssembler = schemaFieldAssembler.name("f" + 1)
            .type()
            .array()
            .items()
            .record("test")
            .fields()
            .name("element")
            .type()
            .map()
            .values()
            .nullable()
            .record("test2")
            .fields()
            .requiredInt("field")
            .endRecord()
            .noDefault()
            .endRecord()
            .noDefault();
          break;
        case "LIST_MAP_NULLABLEVAL_LIST_MAP_LIST_STRUCT":
          schemaFieldAssembler = schemaFieldAssembler.name("f1")
            .type()
            .array()
            .items()
            .record("test")
            .fields()
            .name("element")
            .type()
            .map()
            .values()
            .nullable()
            .array()
            .items()
            .record("test2")
            .fields()
            .name("element")
            .type()
            .map()
            .values()
            .array()
            .items()
            .record("test3")
            .fields()
            .name("element")
            .type()
            .record("test4")
            .fields()
            .requiredInt("field")
            .endRecord()
            .noDefault()
            .endRecord()
            .noDefault()
            .endRecord()
            .noDefault()
            .endRecord()
            .noDefault();
          break;
        case "STRING":
        default:
          schemaFieldAssembler = schemaFieldAssembler.optionalString("f" + i);
          break;
      }

    }
    return schemaFieldAssembler.endRecord();
  }
}
