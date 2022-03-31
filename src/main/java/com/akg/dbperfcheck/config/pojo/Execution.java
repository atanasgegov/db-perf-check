package com.akg.dbperfcheck.config.pojo;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class Execution {
	
	private String what;
	private String mode;
	private int timeInMs;
}
