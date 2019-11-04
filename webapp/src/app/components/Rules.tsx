import {
  faArrowToTop,
  faCalculator,
  faClock,
  faFont,
  faInfoCircle,
  faLaptopCode,
  faLayerGroup,
  IconDefinition,
} from "@fortawesome/pro-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import Axios from "axios";
import { isArray } from "lodash/fp";
import React, { FC } from "react";

import { Badge, Button, CardBody, CardFooter, CardHeader, Table } from "reactstrap";
import styled from "styled-components/macro";
import { Alert, Rule } from "../interfaces";
import { CenteredContainer } from "./CenteredContainer";
import { ScrollingCol } from "./App";

const badgeColorMap: {
  [s: string]: string;
} = {
  ACTIVE: "success",
  DELETE: "danger",
  PAUSE: "warning",
};

const iconMap: {
  [s: string]: IconDefinition;
} = {
  aggregateFieldName: faFont,
  aggregatorFunctionType: faCalculator,
  groupingKeyNames: faLayerGroup,
  limit: faArrowToTop,
  limitOperatorType: faLaptopCode,
  windowMinutes: faClock,
};

const seperator: {
  [s: string]: string;
} = {
  EQUAL: "to",
  GREATER: "than",
  GREATER_EQUAL: "than",
  LESS: "than",
  LESS_EQUAL: "than",
  NOT_EQUAL: "to",
};

const RuleTitle = styled.div`
  display: flex;
  align-items: center;
`;

const RuleTable = styled(Table)`
  && {
    width: calc(100% + 1px);
    border: 0;
    margin: 0;

    td {
      vertical-align: middle !important;

      &:first-child {
        border-left: 0;
      }

      &:last-child {
        border-right: 0;
      }
    }

    tr:first-child {
      td {
        border-top: 0;
      }
    }
  }
`;

const fields = [
  "aggregatorFunctionType",
  "aggregateFieldName",
  "groupingKeyNames",
  "limitOperatorType",
  "limit",
  "windowMinutes",
];

// const omitFields = omit(["ruleId", "ruleState", "unique"]);

const hasAlert = (alerts: Alert[], rule: Rule) => alerts.some(alert => alert.ruleId === rule.id);

export const Rules: FC<Props> = props => {
  const handleDelete = (id: number) => () => {
    Axios.delete(`/api/rules/${id}`).then(props.clearRule(id));
  };

  const triggerAlert = (id: number) => () => fetch(`/api/rules/${id}/alert`).catch();
  const tooManyRules = props.rules.length > 3;

  return (
    <ScrollingCol xs={{ size: 5, offset: 1 }} onScroll={props.handleScroll}>
      {props.rules.map(rule => {
        const payload = JSON.parse(rule.rulePayload);

        return (
          <CenteredContainer
            ref={rule.ref}
            key={rule.id}
            tooManyItems={tooManyRules}
            style={{
              borderColor: hasAlert(props.alerts, rule) ? "#dc3545" : undefined,
              borderWidth: hasAlert(props.alerts, rule) ? 2 : 1,
            }}
          >
            <CardHeader className="d-flex justify-content-between align-items-center" style={{ padding: "0.3rem" }}>
              <RuleTitle>
                <FontAwesomeIcon icon={faInfoCircle} fixedWidth={true} className="mr-2" />
                Rule #{rule.id}{" "}
                <Badge color={badgeColorMap[payload.ruleState]} className="ml-2">
                  {payload.ruleState}
                </Badge>
              </RuleTitle>
              <Button size="sm" color="warning" onClick={triggerAlert(rule.id)} className="ml-auto mr-2">
                Alert
              </Button>
              <Button size="sm" color="danger" outline={true} onClick={handleDelete(rule.id)}>
                Delete
              </Button>
            </CardHeader>
            <CardBody className="p-0">
              <RuleTable size="sm" bordered={true}>
                <tbody>
                  {fields.map(key => {
                    const field = payload[key];
                    return (
                      <tr key={key}>
                        <td style={{ width: 10 }}>
                          <FontAwesomeIcon icon={iconMap[key]} fixedWidth={true} />
                        </td>
                        <td style={{ width: 30 }}>{key}</td>
                        <td>{isArray(field) ? field.map(v => `"${v}"`).join(", ") : field}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </RuleTable>
            </CardBody>
            <CardFooter style={{ padding: "0.3rem" }}>
              <em>{payload.aggregatorFunctionType}</em> of <em>{payload.aggregateFieldName}</em> aggregated by "
              <em>{payload.groupingKeyNames.join(", ")}</em>" is <em>{payload.limitOperatorType}</em>{" "}
              {seperator[payload.limitOperatorType]} <em>{payload.limit}</em> within an interval of{" "}
              <em>{payload.windowMinutes}</em> minutes.
            </CardFooter>
          </CenteredContainer>
        );
      })}
    </ScrollingCol>
  );
};

interface Props {
  alerts: Alert[];
  rules: Rule[];
  clearRule: (id: number) => () => void;
  handleScroll: () => void;
}
