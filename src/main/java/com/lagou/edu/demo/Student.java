package com.lagou.edu.demo;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * @author nandy
 * @create 2021/3/2 18:54
 */
@Setter
@Getter
public class Student implements Serializable {

    private static final long serialVersionUID = -3247106837870523911L;

    private int id;

    private String name;

    private int age;

    private String createDate;
}

