package com.owl.mq.server.push.service;

import com.owl.mq.server.bo.ControlResult;

/**
 * @Author: Tboy
 */
public interface FlowController<T> {

    ControlResult flowControl(T t);
}
