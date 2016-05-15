/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package soayjoni.jdbcutil.util;

/**
 *
 * @author mashuk
 * @param <V>
 */
public class Param<V> {

    private final int index;
    private final ParamType paramType;
    private final String name;
    private final V value;

    public Param(int index, ParamType paramType, String name, V value) {
        this.index = index;
        this.paramType = paramType;
        this.name = name;
        this.value = value;
    }

    public ParamType getParamType() {
        return paramType;
    }

    public int getIndex() {
        return index;
    }

    public String getName() {
        return name;
    }

    public V getValue() {
        return value;
    }

}
