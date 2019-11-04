import React, { FC } from "react";
import { Button, CardBody, CardHeader } from "reactstrap";
import { Alert } from "../interfaces";
import { CenteredContainer } from "./CenteredContainer";
import { ScrollingCol } from "./App";

export const Alerts: FC<Props> = props => {
  const tooManyAlerts = props.alerts.length > 4;

  return (
    <ScrollingCol xs={{ size: 3, offset: 1 }} onScroll={props.handleScroll}>
      {props.alerts.map((alert, idx) => {
        return (
          <CenteredContainer key={idx} className="w-100" ref={alert.ref} tooManyItems={tooManyAlerts}>
            <CardHeader>
              Alert
              <Button size="sm" color="primary" onClick={props.clearAlert(idx)} className="ml-3">
                Clear Alert
              </Button>
            </CardHeader>
            <CardBody>
              <ul>
                <li>rule: {alert.ruleId}</li>
                <li>from: {alert.triggeringEvent.payeeId}</li>
                <li>amount: {alert.triggeringValue}</li>
                <li>to: {alert.triggeringEvent.beneficiaryId}</li>
                <li>transaction: {alert.triggeringEvent.transactionId}</li>
              </ul>
            </CardBody>
          </CenteredContainer>
        );
      })}
    </ScrollingCol>
  );
};

interface Props {
  alerts: Alert[];
  clearAlert: any;
  handleScroll: () => void;
}
