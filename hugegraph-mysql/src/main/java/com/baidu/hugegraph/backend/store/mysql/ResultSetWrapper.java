package com.baidu.hugegraph.backend.store.mysql;

import com.baidu.hugegraph.backend.BackendException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ResultSetWrapper implements AutoCloseable{

    private final ResultSet resultSet;
    private final Statement statement;

    public ResultSetWrapper(ResultSet resultSet, Statement statement) {
        this.resultSet = resultSet;
        this.statement = statement;
    }

    public boolean next() throws SQLException {
        return !this.resultSet.isClosed() && this.resultSet.next();
    }

    public void close() {
        try {
            if (this.resultSet != null) {
                this.resultSet.close();
            }

            if (this.statement != null) {
                this.statement.close();
            }
        } catch (SQLException e) {
            throw new BackendException(
                    "Failed to close resultSet and statement", e);
        }
    }

    public ResultSet resultSet() {
        return resultSet;
    }
    
}
