package org.acme.services;

import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.acme.beans.Product;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import org.acme.beans.Order;
import org.acme.beans.OrderEntry;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

@ApplicationScoped
public class StockShipmentService {

    private static final Logger LOGGER = Logger.getLogger("StockShipmentService");

    @Inject
    @Channel("shipments-out")
    Emitter<Order> emitter;

    public void shipStock(String orderCode, Product product, Integer amount) {

        if (orderCode == null || orderCode.isEmpty()) {
            orderCode = "FULFILLMENT";
        }
        LOGGER.log(Level.INFO, "Updating sku:{0} for {1} items.", new Object[]{product.getProductSku(), amount});
        OrderEntry entry = new OrderEntry(product, amount);
        Order shipment = new Order(orderCode, new OrderEntry[]{entry});
        emitter.send(shipment);
    }
}
