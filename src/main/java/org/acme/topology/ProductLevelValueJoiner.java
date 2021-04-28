/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.acme.topology;

import org.acme.beans.Product;
import org.apache.kafka.streams.kstream.ValueJoiner;

/**
 *
 * @author dhawkins
 */
public class ProductLevelValueJoiner implements ValueJoiner<Integer, Integer, Integer> {

    @Override
    public Integer apply(Integer lhs, Integer rhs) {
        return lhs + rhs;
    }
    
}
