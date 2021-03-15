package org.puppy.hive.auth.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.AuthenticationException;

/**
 * 自定义验证hive用户名和密码的逻辑
 *
 * @author puppy(陶江航 taojianghangdsjb @ smegz.cn)
 * @since 2021/3/12 17:24
 */
public class BasicUsernamePasswdAuthenticator implements PasswdAuthenticationProvider {
    private final static Logger LOGGER = LoggerFactory.getLogger(BasicUsernamePasswdAuthenticator.class);
    private static final String HIVE_JDBC_PASSWD_AUTH_PREFIX = "hive.jdbc.passwd.%s";


    private Configuration conf = null;

    @Override
    public void Authenticate(String user, String password) throws AuthenticationException {
        LOGGER.info("user: " + user + " try login.");
        String passwdFromConf = getConf().get(String.format(HIVE_JDBC_PASSWD_AUTH_PREFIX, user));
        LOGGER.info("读取到用户{}的配置密码为{},传入密码为{}", user, passwdFromConf, password);
        if (passwdFromConf == null) {
            String message = "user's ACL configuration is not found. user:" + user + ",passwdFromConf:" + passwdFromConf;
            LOGGER.info(message);
            throw new AuthenticationException(message);
        }
        if (!passwdFromConf.equals(password)) {
            String message = "user name and password is mismatch. user:" + user + ",passwdFromConf:" + passwdFromConf;
            LOGGER.error(message);
            throw new AuthenticationException(message);
        }
        LOGGER.info("认证通过");
    }


    public Configuration getConf() {
        if (conf == null) {
            this.conf = new Configuration(new HiveConf());
        }
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }
}
