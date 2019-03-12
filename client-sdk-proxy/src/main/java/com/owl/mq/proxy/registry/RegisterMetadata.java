package com.owl.mq.proxy.registry;


import com.owl.mq.proxy.transport.Address;

import java.io.Serializable;
import java.util.Objects;

/**
 * @Author: Tboy
 */
public class RegisterMetadata implements Serializable {

    private String path;

    private Address address;

    public RegisterMetadata(){
        //
    }

    public RegisterMetadata(String path, Address address){
        this.path = path;
        this.address = address;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Address getAddress() {
        return address;
    }

    public void setAddress(Address address) {
        this.address = address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegisterMetadata that = (RegisterMetadata) o;
        return Objects.equals(getPath(), that.getPath()) &&
                Objects.equals(getAddress(), that.getAddress());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPath(), getAddress());
    }

    @Override
    public String toString() {
        return "RegisterMetadata{" +
                "path='" + path + '\'' +
                ", address=" + address +
                '}';
    }
}
