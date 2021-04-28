package org.acme.services;

import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.acme.beans.Product;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import org.acme.beans.Shipment;
import org.acme.beans.ShipmentLineEntry;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

@ApplicationScoped
public class StockShipmentService {

    private static final Logger LOGGER = Logger.getLogger("StockShipmentService");

    @Inject
    @Channel("shipments-out")
    Emitter<Shipment> emitter;

    public void shipStock(String orderId, Product product, Integer amount) {

        if (orderId == null || orderId.isEmpty()) {
            orderId = UUID.randomUUID().toString();
        }
        LOGGER.log(Level.INFO, "Updating sku:{0} for {1} items.", new Object[]{product.getProductSku(), amount});
        ShipmentLineEntry entry = new ShipmentLineEntry(product, amount);
        Shipment shipment = new Shipment(orderId, new ShipmentLineEntry[]{entry});
        emitter.send(shipment);
    }
}
