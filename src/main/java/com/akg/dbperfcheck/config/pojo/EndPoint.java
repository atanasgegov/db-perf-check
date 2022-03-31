package com.akg.dbperfcheck.config.pojo;

import lombok.Data;

@Data
public class EndPoint {
	
	private String search;
	private String insert;
	private String update;
	private String delete;
}
