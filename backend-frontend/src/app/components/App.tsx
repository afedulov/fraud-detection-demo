import { Header, Alerts, Rules, Transactions } from "app/components";
import { Rule, Alert } from "app/interfaces";
import { useLines } from "app/utils/useLines";
import Axios from "axios";
import React, { createRef, FC, useEffect, useRef, useState } from "react";
import { Col, Container, Row } from "reactstrap";
import styled from "styled-components/macro";
import SockJsClient from "react-stomp";
import uuid from "uuid/v4";
import "../assets/app.scss";

const LayoutContainer = styled.div`
  display: flex;
  flex-direction: column;
  width: 100%;
  max-height: 100vh;
  height: 100vh;
  overflow: hidden;
`;

export const ScrollingCol = styled(Col)`
  overflow-y: scroll;
  max-height: 100%;
  display: flex;
  flex-direction: column;
`;

export const App: FC = () => {
  const [rules, setRules] = useState<Rule[]>([]);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const transactionsRef = useRef<HTMLDivElement>(null);
  const { handleScroll } = useLines(transactionsRef, rules, alerts);

  useEffect(() => {
    Axios.get<Rule[]>("/api/rules").then(response =>
      setRules(response.data.map(rule => ({ ...rule, ref: createRef<HTMLDivElement>() })))
    );
  }, []);

  const clearRule = (id: number) => () => setRules(rules.filter(rule => id !== rule.id));

  const clearAlert = (id: number) => () => {
    setAlerts(state => {
      const newAlerts = [...state];
      newAlerts.splice(id, 1);
      return newAlerts;
    });
  };

  const handleMessage = (alert: Alert) => {
    const alertId = uuid();
    const newAlert = {
      ...alert,
      alertId,
      ref: createRef<HTMLDivElement>(),
      timeout: setTimeout(() => setAlerts(state => state.filter(a => a.alertId !== alertId)), 5 * 1000),
    };

    setAlerts((state: Alert[]) => {
      const filteredState = state.filter(a => a.ruleId !== alert.ruleId);
      return [...filteredState, newAlert].sort((a, b) => (a.ruleId > b.ruleId ? 1 : -1));
    });
  };

  const handleLatencyMessage = (latency: string) => {
    // tslint:disable-next-line: no-console
    console.info(latency);
  };

  return (
    <>
      <SockJsClient url="/ws/backend" topics={["/topic/alerts"]} onMessage={handleMessage} />
      <SockJsClient url="/ws/backend" topics={["/topic/latency"]} onMessage={handleLatencyMessage} />
      <LayoutContainer>
        <Header setRules={setRules} />
        <Container fluid={true} className="flex-grow-1 d-flex w-100 flex-column overflow-hidden">
          <Row className="flex-grow-1 overflow-hidden">
            <Transactions ref={transactionsRef} />
            <Rules clearRule={clearRule} rules={rules} alerts={alerts} handleScroll={handleScroll} />
            <Alerts alerts={alerts} clearAlert={clearAlert} handleScroll={handleScroll} />
          </Row>
        </Container>
      </LayoutContainer>
    </>
  );
};
