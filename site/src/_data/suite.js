// $data.suite: how the suite's tests distribute across operations, so the
// methodology's coverage-weighting sentence states counts it has read rather
// than counts someone typed. Committed artefact, no fetch.
import { buildSuiteShape } from "../../lib/suite-shape.mjs";

export default function () {
  return buildSuiteShape();
}
