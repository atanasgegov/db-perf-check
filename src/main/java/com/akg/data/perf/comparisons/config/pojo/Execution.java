package com.akg.data.perf.comparisons.config.pojo;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class Execution {
	
	private String what;
	private String mode;
	private int timeInMs;
}
