package com.slg.game.application.execution;

/** 操作阶段不是存储提交结论；COMMITTING 可含 UNKNOWN，ISOLATED 仍持有全部资格。 */
public enum OperationPhase { WAITING, PREPARING, COMMITTING, PUBLISHING, REPAIRING, ISOLATED, SETTLED }
