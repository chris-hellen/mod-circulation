package org.folio.circulation.resources;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.circulation.domain.InstanceExtended;
import org.folio.circulation.infrastructure.storage.SearchRepository;
import org.folio.circulation.support.Clients;
import org.folio.circulation.support.http.server.JsonHttpResponse;
import org.folio.circulation.support.http.server.WebContext;
import java.lang.invoke.MethodHandles;
import java.util.List;

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
    List<String> queryParams = routingContext.queryParam("query");
    if (!queryParams.isEmpty()) {
      String query = queryParams.get(0);
      final var searchRepository = new SearchRepository(clients);
      searchRepository.getInstanceWithItems(query)
        .thenApply(r -> r.map(this::toJson))
        .thenApply(r -> r.map(JsonHttpResponse::ok))
        .thenAccept(context::writeResultToHttpResponse);
    }
  }

  private JsonObject toJson(InstanceExtended instanceExtended) {
    if (instanceExtended != null)
      return instanceExtended.toJson();
    return new JsonObject();
  }
}
