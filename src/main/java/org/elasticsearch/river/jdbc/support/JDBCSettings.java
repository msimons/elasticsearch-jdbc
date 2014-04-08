package org.elasticsearch.river.jdbc.support;

/**
 * Object holding JDBC Settings
 * @author marcosimons
 */
public class JDBCSettings {
    public String driver;
    public String url;
    public String user;
    public String password;

    public JDBCSettings(String driver, String url, String user, String password) {
        super();
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JDBCSettings that = (JDBCSettings) o;

        if (!driver.equals(that.driver)) return false;
        if (!password.equals(that.password)) return false;
        if (!url.equals(that.url)) return false;
        if (!user.equals(that.user)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = driver.hashCode();
        result = 31 * result + url.hashCode();
        result = 31 * result + user.hashCode();
        result = 31 * result + password.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "JDBCSettings{" +
                "driver='" + driver + '\'' +
                ", url='" + url + '\'' +
                ", user='" + user + '\'' +
                ", password='" + password + '\'' +
                '}';
    }
}