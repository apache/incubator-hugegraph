package com.baidu.hugegraph.task;

public enum TaskPriority {

    EMERGENCY(0),
    HIGH(1),
    NORMAL(2),
    LOW(3)
    ;
    
    private final int value;
    private TaskPriority(int value) {
        this.value = value;
    }

    public int getValue() {
        return this.value;
    }

    public static TaskPriority fromValue(int value) {
        switch (value) {
        case 0:
                return TaskPriority.EMERGENCY;
        case 1:
                return TaskPriority.HIGH;
        case 2:
                return TaskPriority.NORMAL;
        case 3:
                return TaskPriority.LOW;
        default:
            return TaskPriority.LOW;
        }
    }
    
}
