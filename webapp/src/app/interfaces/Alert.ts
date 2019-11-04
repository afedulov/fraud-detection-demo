import { Transaction } from "./Transaction";
import { RefObject } from "react";

export interface Alert {
  alertId: string;
  ruleId: number;
  rulePayload: string;
  triggeringValue: number;
  triggeringEvent: Transaction;
  ref: RefObject<HTMLDivElement>;
}
