import { setupMetricsEndpoints } from "__support__/server-mocks";
import {
  renderWithProviders,
  screen,
  waitForLoaderToBeRemoved,
} from "__support__/ui";
import LoadingAndErrorWrapper from "metabase/components/LoadingAndErrorWrapper";
import { createMockMetric } from "metabase-types/api/mocks";

import { useMetricQuery } from "./use-metric-query";

const TEST_METRIC = createMockMetric();

const TestComponent = () => {
  const { data, isLoading, error } = useMetricQuery({
    id: TEST_METRIC.id,
  });

  if (isLoading || error || !data) {
    return <LoadingAndErrorWrapper loading={isLoading} error={error} />;
  }

  return <div>{data.name}</div>;
};

const setup = () => {
  setupMetricsEndpoints([TEST_METRIC]);
  renderWithProviders(<TestComponent />);
};

describe("useMetricQuery", () => {
  it("should be initially loading", () => {
    setup();
    expect(screen.getByTestId("loading-spinner")).toBeInTheDocument();
  });

  it("should show data from the response", async () => {
    setup();
    await waitForLoaderToBeRemoved();
    expect(screen.getByText(TEST_METRIC.name)).toBeInTheDocument();
  });
});
