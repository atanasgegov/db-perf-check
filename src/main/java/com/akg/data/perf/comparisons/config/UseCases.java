package com.akg.data.perf.comparisons.config;

import java.util.List;

import lombok.Data;

@Data
public class UseCases {
	private Execution one;
	private List<Execution> crud;
}
