package org.acme;

import java.util.logging.Level;
import java.util.logging.Logger;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.acme.beans.Product;
import org.acme.beans.Shipment;
import org.acme.beans.ShipmentLineEntry;
import org.acme.services.StockShipmentService;

@Path("/shipment")
public class ShipmentResource {

    private static final Logger LOGGER = Logger.getLogger("ShipmentResource");

    @Inject
    StockShipmentService stockShipmentService;

    @GET
    @Path("{sku}/{name}/{amount}/{orderId}")
    public Boolean shipQuantity(@PathParam("sku") String sku, @PathParam("name") String name, @PathParam("amount") Integer amount, @PathParam("orderId") String orderId) {
        LOGGER.log(Level.INFO, "Updating sku:{0} for {1} items.", new Object[]{sku, amount});
        try {
            stockShipmentService.shipStock(orderId, new Product(sku, name), amount);
        } catch (Throwable t) {
            System.err.println(t.getLocalizedMessage());
            return false;
        }
        return true;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{sku}/{orderId}")
    public Boolean shipQuantity(@PathParam("sku") String sku, @PathParam("orderId") String orderId, ShipmentLineEntry entry) {

        /**
         * Example Payload:
         * {"product":{"productName":"bananas","productSku":"1111-2222-3333-1115"},"quantity":254}
         */
        LOGGER.log(Level.INFO, "Updating sku:{0} for {1} items.", new Object[]{entry.getProduct().getProductSku(), entry.getQuantity()});
        try {
            stockShipmentService.shipStock(orderId, entry.getProduct(), entry.getQuantity());
        } catch (Throwable t) {
            System.err.println(t.getLocalizedMessage());
            return false;
        }
        return true;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("")
    public Boolean shipQuantities(Shipment shipment) {

        /**
         * Example Payload:
         * {"supplyCode":"restock","supplyUpdateEntries":[{"product":{"productName":"bananas","productSku":"1111-2222-3333-1115"},"quantity":254},{"product":{"productName":"cherries","productSku":"1111-2222-3333-1122"},"quantity":312}]}
         */
        if (shipment == null || shipment.getShipmentLineEntries() == null || shipment.getShipmentLineEntries().length <= 0) {
            LOGGER.log(Level.INFO, "Invalid shipment request.");
            return false;
        }

        LOGGER.log(Level.INFO, "Shipping {0} item types.", new Object[]{shipment.getShipmentLineEntries().length});

        for (ShipmentLineEntry entry : shipment.getShipmentLineEntries()) {
            LOGGER.log(Level.INFO, "Updating sku:{0} for {1} items.", new Object[]{entry.getProduct().getProductSku(), entry.getQuantity()});
            try {
                stockShipmentService.shipStock(shipment.getOrderId(), entry.getProduct(), entry.getQuantity());
            } catch (Throwable t) {
                System.err.println(t.getLocalizedMessage());
                return false;
            }
        }

        return true;
    }

}
