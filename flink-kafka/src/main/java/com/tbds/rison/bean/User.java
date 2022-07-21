package com.tbds.rison.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @PACKAGE_NAME: com.tbds.rison.bean
 * @NAME: User
 * @USER: Rison
 * @DATE: 2022/7/12 10:19
 * @PROJECT_NAME: flink-tbds-kerberos
 **/
@NoArgsConstructor
@Data
@AllArgsConstructor
public class User {
    private String uid;
    private String name;
    private String code;
}
