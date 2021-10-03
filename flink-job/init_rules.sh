#!/bin/sh


RULES="1,(active),(payeeId&beneficiaryId),,(paymentAmount),(SUM),(>),(20000000),(43200)\n
2,(pause),(paymentType),,(COUNT_FLINK),(SUM),(LESS),(300),(1440)\n
3,(active),(beneficiaryId),,(paymentAmount),(SUM),(>=),(10000000),(1440)\n
4,(active),(paymentType),,(COUNT_WITH_RESET_FLINK),(SUM),(>=),(100),(1440)"
echo $RULES | nc localhost 9999
