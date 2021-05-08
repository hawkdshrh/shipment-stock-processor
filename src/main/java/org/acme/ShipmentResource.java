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
import org.acme.beans.Order;
import org.acme.beans.OrderEntry;
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
    @Path("{sku}/{orderCode}")
    public Boolean shipQuantity(@PathParam("sku") String sku, @PathParam("orderCode") String orderCode, OrderEntry entry) {

        /**
         * Example Payload:
         * {"product":{"productName":"bananas","productSku":"1111-2222-3333-1115"},"quantity":254}
         */
        LOGGER.log(Level.INFO, "Updating sku:{0} for {1} items.", new Object[]{entry.getProduct().getProductSku(), entry.getQuantity()});
        try {
            stockShipmentService.shipStock(orderCode, entry.getProduct(), entry.getQuantity());
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
    public Boolean shipQuantities(Order shipment) {

        /**
         * Example Payload:
         * {"orderId":"103427","shipmentLineEntries":[{"product":{"productName":"peaches","productSku":"1111-2222-3333-1226"},"quantity":-20},{"product":{"productName":"cherries","productSku":"1111-2222-3333-1122"},"quantity":-10}]}
         */
        if (shipment == null || shipment.getOrderEntries() == null || shipment.getOrderEntries().length <= 0) {
            LOGGER.log(Level.INFO, "Invalid shipment request.");
            return false;
        }

        LOGGER.log(Level.INFO, "Shipping {0} item types.", new Object[]{shipment.getOrderEntries().length});

        for (OrderEntry entry : shipment.getOrderEntries()) {
            LOGGER.log(Level.INFO, "Updating sku:{0} for {1} items.", new Object[]{entry.getProduct().getProductSku(), entry.getQuantity()});
            try {
                stockShipmentService.shipStock(shipment.getOrderCode(), entry.getProduct(), entry.getQuantity());
            } catch (Throwable t) {
                System.err.println(t.getLocalizedMessage());
                return false;
            }
        }

        return true;
    }

}
