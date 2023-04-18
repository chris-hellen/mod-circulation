package org.folio.circulation.infrastructure.storage.users;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.circulation.domain.Department;
import org.folio.circulation.domain.MultipleRecords;
import org.folio.circulation.domain.Request;
import org.folio.circulation.support.Clients;
import org.folio.circulation.support.CollectionResourceClient;
import org.folio.circulation.support.http.client.CqlQuery;
import org.folio.circulation.support.http.client.PageLimit;
import org.folio.circulation.support.http.client.Response;
import org.folio.circulation.support.results.Result;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DepartmentRepository {
  private static final Logger log = LogManager.getLogger(MethodHandles.lookup().lookupClass());
  private final CollectionResourceClient departmentClient;
  public static final int BATCH_SIZE = 90;

  public DepartmentRepository(Clients clients) {
    this.departmentClient = clients.departmentClient();
  }

  public List<Department> getDepartmentByIds(List<String> departmentIds) {
    log.info("getDepartmentByIds:: Fetching departmentByIds {}", departmentIds);
    return findDepartmentList(departmentIds).stream()
      .map(Result::value)
      .map(rec -> new ArrayList<>(rec.getRecords()))
      .flatMap(Collection::stream)
      .collect(Collectors.toList());
  }

  public CompletableFuture<Result<MultipleRecords<Request>>> findDepartmentsForRequests(
    MultipleRecords<Request> multipleRequests) {
    log.info("findDepartmentsForRequests:: Fetching departments for multipleRequests {} ", multipleRequests.getTotalRecords());
    Map<String, Department> departmentMap =
      fetchDepartmentsForRequest(multipleRequests.getRecords()).stream()
        .collect(Collectors.toMap(Department::getId, Function.identity(), (x1,x2) -> x1));
    log.info("departmentMap {} ", departmentMap);
    return CompletableFuture.completedFuture(Result.succeeded(multipleRequests.mapRecords
      (req -> req.withDepartments(matchDepartmentsWithRequest(req, departmentMap)))));
  }

  private List<Department> fetchDepartmentsForRequest(Collection<Request> requests) {
    List<String> departmentIds = requests.stream()
      .filter(req -> req.getRequester() != null)
      .map(req -> req.getRequester().getDepartments().stream()
        .map(String.class::cast)
        .collect(Collectors.toList()))
      .flatMap(List::stream)
      .collect(Collectors.toList());
    log.info("fetchDepartmentsForRequest :: departmentIds {} ", departmentIds);
    return getDepartmentByIds(departmentIds);
  }

  private List<Department> matchDepartmentsWithRequest(Request request, Map<String, Department> departmentMap) {
    if (request.getRequester() == null || request.getRequester().getDepartments() == null) {
      return null;
    }
    return request.getRequester().getDepartments()
      .stream()
      .map(String.class::cast)
      .collect(Collectors.toList())
      .stream().map(departmentMap::get)
      .filter(Objects::nonNull)
      .collect(Collectors.toList());
  }

  private List<Result<MultipleRecords<Department>>> findDepartmentList(List<String> departmentIds) {
    return splitIds(departmentIds)
      .stream()
      .map(dep -> findDepartmentByCql(CqlQuery.exactMatchAny("id", dep),
        departmentClient, PageLimit.noLimit()))
      .map(CompletableFuture::join)
      .collect(Collectors.toList());
  }

  private CompletableFuture<Result<MultipleRecords<Department>>> findDepartmentByCql(
    Result<CqlQuery> query, CollectionResourceClient client, PageLimit pageLimit) {
    return query.after(cqlQuery -> client.getMany(cqlQuery, pageLimit))
      .thenApply(resp -> resp.next(this::mapResponseToDepartments));
  }

  private Result<MultipleRecords<Department>> mapResponseToDepartments(Response response) {
    return MultipleRecords.from(response, Department::new, "departments");
  }

  private List<List<String>> splitIds(List<String> departmentIds) {
    int size = departmentIds.size();
    if (size == 0) {
      return new ArrayList<>();
    }
    int fullChunks = (size - 1) / BATCH_SIZE;
    return IntStream.range(0, fullChunks + 1)
      .mapToObj(n ->
        departmentIds.subList(n * BATCH_SIZE, n == fullChunks
          ? size
          : (n + 1) * BATCH_SIZE))
      .collect(Collectors.toList());
  }
}
