package com.dremio.multirowgroupparquetgenerator;

import java.util.HashMap;
import java.util.TreeSet;

/**
 * Source: https://github.com/jjenkov/cli-args/blob/master/src/main/java/com/jenkov/cliargs/CliArgs.java
 */
public class CliArgs {
  private String[] args = null;
  private final HashMap<String, Integer> switchIndexes = new HashMap<>();
  private final TreeSet<Integer> takenIndexes = new TreeSet<>();

  public CliArgs(String[] args) {
    parse(args);
  }

  public void parse(String[] arguments) {
    this.args = arguments;
    //locate switches.
    switchIndexes.clear();
    takenIndexes.clear();
    for (int i = 0; i < args.length; i++) {
      if (args[i].startsWith("-")) {
        switchIndexes.put(args[i], i);
        takenIndexes.add(i);
      }
    }
  }

  public String switchValue(String switchName, String defaultValue) {
    if (!switchIndexes.containsKey(switchName)) return defaultValue;

    int switchIndex = switchIndexes.get(switchName);
    if (switchIndex + 1 < args.length) {
      takenIndexes.add(switchIndex + 1);
      return args[switchIndex + 1];
    }
    return defaultValue;
  }

  public Integer switchIntValue(String switchName, Integer defaultValue) {
    String switchValue = switchValue(switchName, null);

    if (switchValue == null) return defaultValue;
    return Integer.parseInt(switchValue);
  }
}
