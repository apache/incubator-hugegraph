package com.baidu.hugegraph.backend.store.mysql;

import com.baidu.hugegraph.backend.BackendException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ResultSetWrapper {

    private ResultSet resultSet;
    private Statement statement;

    public ResultSetWrapper(ResultSet resultSet, Statement statement) {
        this.resultSet = resultSet;
        this.statement = statement;
    }

    public boolean next() throws SQLException {
        return resultSet.next();
    }

    public boolean isClosed() throws SQLException {
        return resultSet.isClosed();
    }

    public void close() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }

            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            throw new BackendException(
                    "Failed to close resultSet and statement", e);
        }
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    public void setResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    public Statement getStatement() {
        return statement;
    }

    public void setStatement(Statement statement) {
        this.statement = statement;
    }
}
