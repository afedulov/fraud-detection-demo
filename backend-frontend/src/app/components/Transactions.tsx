import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import React, { forwardRef, useEffect, useRef, useState } from "react";
import {
  faArrowRight,
  faCreditCard,
  faMoneyBill,
  faQuestionCircle,
  IconDefinition,
} from "@fortawesome/pro-regular-svg-icons";
import { Badge, Card, CardBody, CardHeader, Col } from "reactstrap";
import styled from "styled-components/macro";
import { Transaction } from "../interfaces";
import Slider from "react-rangeslider";
import { useLocalStorage, useUpdateEffect } from "react-use";
import { AutoSizer, List, ListRowRenderer } from "react-virtualized";
import SockJsClient from "react-stomp";
import "react-virtualized/styles.css";

// MSG
// beneficiaryId: 42694
// eventTime: 1565965071385
// payeeId: 20908
// paymentAmount: 13.54
// paymentType: "CRD"
// transactionId: 5954524216210268000

const paymentTypeMap: {
  [s: string]: IconDefinition;
} = {
  CRD: faCreditCard,
  CSH: faMoneyBill,
  undefined: faQuestionCircle,
};

const TransactionsCard = styled(Card)`
  width: 100%;
  height: 100%;
  border-left: 0 !important;

  .rangeslider__handle {
    &:focus {
      outline: 0;
    }
  }
`;

const TransactionsHeading = styled.div`
  display: flex;
  justify-content: space-between;
  border-bottom: 1px solid rgba(0, 0, 0, 0.125);
  font-weight: 500;
`;

const Payment = styled.div`
  position: relative;
  display: flex;
  align-items: center;
  justify-content: space-around;
  height: 50px;
  border-top: 1px solid rgba(0, 0, 0, 0.125);

  &.text-danger {
    border-top: 1px solid #dc3545;
    border-bottom: 1px solid #dc3545;
  }

  &.text-danger + & {
    border-top: 0;
  }

  &:first-of-type {
    border-top: none;
  }
`;

const FlexSpan = styled.span`
  display: inline-flex;
  align-items: center;
  width: 100px;
  flex-basis: 33%;
  flex: 1 1 auto;
`;

const Payee = styled(FlexSpan)`
  justify-content: flex-start;
`;

const Details = styled(FlexSpan)`
  justify-content: center;
`;

const Beneficiary = styled(FlexSpan)`
  justify-content: flex-end;
`;

export const Transactions = forwardRef<HTMLDivElement, {}>((props, ref) => {
  const list = useRef<List>(null);
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  const addTransaction = (transaction: Transaction) => setTransactions(state => [...state.slice(-50), transaction]);

  const [value, setValue] = useLocalStorage("generatorSpeed", 1);
  const handleSliderChange = (val: number) => setValue(val);

  useUpdateEffect(() => {
    fetch(`/api/generatorSpeed/${value}`);
  }, [value]);

  useEffect(() => {
    if (list.current) {
      list.current.scrollToRow(transactions.length);
    }
  }, [transactions]);

  const renderRow: ListRowRenderer = ({ key, index, style }) => {
    const t = transactions[index];

    return (
      <Payment key={key} style={style} className="px-2">
        <Payee>{t.payeeId}</Payee>
        <Details>
          <FontAwesomeIcon className="mx-1" icon={paymentTypeMap[t.paymentType]} />
          <Badge color="info">${parseFloat(t.paymentAmount.toString()).toFixed(2)}</Badge>
          <FontAwesomeIcon className="mx-1" icon={faArrowRight} />
        </Details>
        <Beneficiary>{t.beneficiaryId}</Beneficiary>
      </Payment>
    );
  };

  return (
    <>
      <SockJsClient url="/ws/backend" topics={["/topic/transactions"]} onMessage={addTransaction} />
      <Col xs="2" className="d-flex flex-column px-0">
        <TransactionsCard innerRef={ref}>
          <CardHeader className="d-flex align-items-center py-0 justify-content-between">
            <div style={{ width: 200 }} className="mr-3 d-inline-block">
              <Slider value={value} onChange={handleSliderChange} max={100} min={0} tooltip={false} step={1} />
            </div>
            <span>{value}</span>
          </CardHeader>
          <CardBody className="p-0 mb-0" style={{ height: 919 }}>
            <TransactionsHeading className="px-2 py-1">
              <span>Payee</span>
              <span>Amount</span>
              <span>Beneficiary</span>
            </TransactionsHeading>
            <AutoSizer>
              {({ height, width }) => (
                <List
                  ref={list}
                  height={height}
                  width={width}
                  rowHeight={40}
                  rowCount={transactions.length - 1}
                  rowRenderer={renderRow}
                />
              )}
            </AutoSizer>
          </CardBody>
        </TransactionsCard>
      </Col>
    </>
  );
});
