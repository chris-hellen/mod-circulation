package org.folio.circulation.resources;

import java.lang.invoke.MethodHandles;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.circulation.domain.SearchInstance;
import org.folio.circulation.infrastructure.storage.SearchRepository;
import org.folio.circulation.support.Clients;
import org.folio.circulation.support.http.server.JsonHttpResponse;
import org.folio.circulation.support.http.server.WebContext;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class ItemsByInstanceResource extends Resource {

  private static final Logger log = LogManager.getLogger(MethodHandles.lookup().lookupClass());

  public ItemsByInstanceResource(HttpClient client) {
    super(client);
  }

  @Override
  public void register(Router router) {
    router.get("/circulation/items-by-instance")
      .handler(this::getInstanceItems);
  }

  private void getInstanceItems(RoutingContext routingContext) {
    final WebContext context = new WebContext(routingContext);
    final Clients clients = Clients.create(context, client);
      new SearchRepository(clients).getInstanceWithItems(routingContext.queryParam("query"))
        .thenApply(r -> r.map(this::toJson))
        .thenApply(r -> r.map(JsonHttpResponse::ok))
        .thenAccept(context::writeResultToHttpResponse);
  }

  private JsonObject toJson(SearchInstance instanceExtended) {
    log.debug("toJson:: instanceExtended: {}", () -> instanceExtended);
    if (instanceExtended != null) {
      return instanceExtended.toJson();
    }
    return new JsonObject();
  }
}
