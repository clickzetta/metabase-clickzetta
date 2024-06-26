import { fireEvent } from "@testing-library/react";
import { Route } from "react-router";

import {
  setupPublicCardQueryEndpoints,
  setupPublicQuestionEndpoints,
} from "__support__/server-mocks";
import { renderWithProviders, screen, within } from "__support__/ui";
import registerVisualizations from "metabase/visualizations/register";
import type { VisualizationProps } from "metabase/visualizations/types";
import {
  createMockPublicCard,
  createMockEmbedDataset,
} from "metabase-types/api/mocks";
import { createMockState } from "metabase-types/store/mocks";

import { PublicQuestion } from "./PublicQuestion";

registerVisualizations();

const FAKE_UUID = "123456";

const QUESTION_NAME = "Public question";

const VisualizationMock = ({
  onUpdateVisualizationSettings,
  rawSeries,
}: VisualizationProps) => {
  const [
    {
      card,
      data: { rows },
    },
  ] = rawSeries;

  return (
    <div>
      <div>
        {rows[0].map((value, i) => (
          <span key={i}>{value}</span>
        ))}
      </div>
      <div data-testid="settings">
        {JSON.stringify(card.visualization_settings)}
      </div>
      <button onClick={() => onUpdateVisualizationSettings({ foo: "bar" })}>
        update settings
      </button>
    </div>
  );
};

jest.mock(
  "metabase/visualizations/components/Visualization",
  () => VisualizationMock,
);

async function setup() {
  setupPublicQuestionEndpoints(
    FAKE_UUID,
    createMockPublicCard({ name: QUESTION_NAME }),
  );
  setupPublicCardQueryEndpoints(
    FAKE_UUID,
    createMockEmbedDataset({
      data: { rows: [["John W."]] },
    }),
  );

  renderWithProviders(
    <Route path="public/question/:uuid" component={PublicQuestion} />,
    {
      storeInitialState: createMockState(),
      withRouter: true,
      initialRoute: `public/question/${FAKE_UUID}`,
    },
  );
  expect(await screen.findByText(QUESTION_NAME)).toBeInTheDocument();
}

describe("PublicQuestion", () => {
  it("should render data", async () => {
    await setup();
    expect(screen.getByText("John W.")).toBeInTheDocument();
  });

  it("should not database prompt banner", async () => {
    await setup();

    // Since database prompt banner render as a banner. If we only find one banner
    // that is showing the question name, we know that the database prompt banner is not showing.
    expect(
      within(screen.getByRole("banner")).getByText(QUESTION_NAME),
    ).toBeInTheDocument();
  });

  it("should update card settings when visualization component changes them (metabase#37429)", async () => {
    await setup();

    fireEvent.click(
      screen.getByRole("button", {
        name: /update settings/i,
      }),
    );

    expect(screen.getByTestId("settings")).toHaveTextContent(
      JSON.stringify({ foo: "bar" }),
    );
  });
});
