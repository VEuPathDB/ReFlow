package org.gusdb.workflow;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.List;

public class RunnableWorkflowStep extends WorkflowStep {

    boolean isInvoked;
    int invokedButNotRunningCount;

    int handleChangesSinceLastSnapshot()
            throws SQLException, IOException, InterruptedException {
        if (workflow_step_id == null)
            Utilities.error("Step " + getFullName()
                    + " is not found in the database");
        if (getOperativeStateHandled()) {
            if (getOperativeState().equals(Workflow.RUNNING)) {
                String cmd = "ps -p " + process_id;
                Process process = Runtime.getRuntime().exec(cmd);
                process.waitFor();
                if (process.exitValue() != 0) handleMissingProcess();
                process.destroy();
            }
        } else { // this step has been changed by wrapper or pilot UI. log
                 // change.
            if (!getOperativeState().equals(prevState)) {
              String skippedMsg = getSkipped()? "-skipped-" : "";
                steplog(getOperativeState(), skippedMsg);
                isInvoked = false;
            }
            if (off_line != prevOffline)
                steplog("", (off_line ? "OFFLINE" : "ONLINE"));
            if (stop_after != prevStopAfter)
                steplog("", (stop_after ? "STOP_AFTER" : "RESUME"));
            setHandledFlag();
        }
        return getOperativeState().equals(Workflow.RUNNING) ? 1 : 0;
    }

    private void setHandledFlag() throws SQLException {
        // check that state is still as expected, to avoid theoretical race
        // condition

        int offlineInt = off_line ? 1 : 0;
        int stopafterInt = stop_after ? 1 : 0;
        String sql;
        String workflowStepTable = workflowGraph.getWorkflow().getWorkflowStepTable();

        if (!getUndoing()) {
            sql = "UPDATE " + workflowStepTable
                    + " SET state_handled = 1, last_handled_time = " + workflowGraph.getWorkflow().getDbPlatform().getSysdateIdentifier()
                    + " WHERE workflow_step_id = " + workflow_step_id
                    + " AND state = '" + state + "'" + " AND off_line = "
                    + offlineInt + " AND stop_after = " + stopafterInt;
            state_handled = true; // till next snapshot
        } else {
            sql = "UPDATE "
                    + workflowStepTable
                    + " SET undo_state_handled = 1, undo_last_handled_time = " + workflowGraph.getWorkflow().getDbPlatform().getSysdateIdentifier()
                    + " WHERE workflow_step_id = " + workflow_step_id;
            undo_state_handled = true; // till next snapshot
        }
        executeSqlUpdate(sql);
    }

    private void handleMissingProcess() throws SQLException, IOException {
        String workflowStepTable = workflowGraph.getWorkflow().getWorkflowStepTable();
        String sql = "SELECT " + (getUndoing() ? "undo_state" : "state")
                + " FROM " + workflowStepTable + " WHERE workflow_step_id = "
                + workflow_step_id;

        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = workflowGraph.getWorkflow().getDbConnection().createStatement();
            rs = stmt.executeQuery(sql);
            rs.next();
            String stateNow = rs.getString(1);
            if (stateNow.equals(Workflow.RUNNING)) {
                String handleColumn = getUndoing() ? "undo_state_handled"
                        : "state_handled";
                String stateColumn = getUndoing() ? "undo_state" : "state";

                String endTimeString = "end_time = "
                        + workflowGraph.getWorkflow().getDbPlatform().getNvlFunctionName()
                        +"(end_time, "+ workflowGraph.getWorkflow().getDbPlatform().getSysdateIdentifier()+") ";
                sql = "UPDATE " + workflowStepTable + " SET " + stateColumn
                        + " = '" + Workflow.FAILED + "', " + handleColumn
                        + "= 1" + "," + "process_id = null, "
                        + endTimeString
                        + " WHERE workflow_step_id = " + workflow_step_id
                        + " AND " + (getUndoing() ? "undo_state" : "state")
                        + "= '" + Workflow.RUNNING + "'";
                executeSqlUpdate(sql);
                steplog(Workflow.FAILED, "***");
            }
        }
        finally {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
        }
    }

    // if this step is ready, and all parents are done, transition to ON_DECK
    void maybeGoToOnDeck() throws SQLException, IOException {

      // (ignore off_line or stop_after if undoing)

        if (!getOperativeState().equals(Workflow.READY) || (off_line && !getUndoing())) return;

        for (WorkflowStep parent : getParents()) {
            if (!parent.getOperativeState().equals(Workflow.DONE)
                || (parent.getStopAfter() && !getUndoing())) return;
        }

        steplog(Workflow.ON_DECK, "");

        String workflowStepTable = workflowGraph.getWorkflow().getWorkflowStepTable();
        String set = getUndoing() ? " SET undo_state = '" + Workflow.ON_DECK
                + "', undo_state_handled = 1" : " SET state = '"
                + Workflow.ON_DECK + "', state_handled = 1";

        String and = getUndoing() ? "undo_state" : "state";

        String sql = "UPDATE " + workflowStepTable + set
                + " WHERE workflow_step_id = " + workflow_step_id + " AND "
                + and + " = '" + Workflow.READY + "'";
        executeSqlUpdate(sql);
    }

    // if this step doesn't have an invoker (ie, it is a call to or return
    // from a subgraph), just go to done
    void goToDone() throws SQLException {
        String workflowStepTable = workflowGraph.getWorkflow().getWorkflowStepTable();
        String set = getUndoing() ? " SET undo_state = '" + Workflow.DONE
                + "', undo_state_handled = 1, state = '" + Workflow.READY
                + "', state_handled = 1" : " SET state = '" + Workflow.DONE
                + "', state_handled = 1";

        String sql = "UPDATE " + workflowStepTable + set
                + " WHERE workflow_step_id = " + workflow_step_id;
        executeSqlUpdate(sql);
    }

    // try to run a single ON_DECK step
    int runOnDeckStep(Workflow<RunnableWorkflowStep> workflow, boolean testOnly)
            throws IOException, SQLException {

        if (getOperativeState().equals(Workflow.ON_DECK) && !off_line) {
            if (invokerClassName == null) {
                if (subgraphXmlFileName != null) steplog(Workflow.DONE,
                        "(call)");
                else steplog(Workflow.DONE, "");
                goToDone();
            } else {
                String[] cmd = { "idle", "workflowRunStep", workflow.getHomeDir(),
                    workflow.getId().toString(), getFullName(),
                    "" + getId(), invokerClassName,
                    getStepDir() + "/step.err", testOnly ? "test" : "run",
                        getUndoing() ? "1" : "0",
                            getSkipIfFileName()
                };
                List<String> cmd2 = new ArrayList<String>();
                Collections.addAll(cmd2, cmd);
                for (String name : paramValues.keySet()) {
                    String valueStr = paramValues.get(name);
                    valueStr = valueStr.replaceAll("\"", "\\\\\"");
                    cmd2.add("-" + name);
                    cmd2.add("\"" + valueStr + "\"");
                }

                // join wrapper command into a single string
                StringBuilder sb = new StringBuilder();
                String delim = "";
                for (String s : cmd2) {
                    sb.append(delim + s);
                    delim = " ";
                }

                // call sh directly to force wrapper into background
                // so it survives a kill of the controller
                String[] cmd3 = { "sh", "-c", sb.toString() + " &" };
                if (isInvoked) {
                    invokedButNotRunningCount++;
                    steplog("Invoked but not running ("
                            + invokedButNotRunningCount + ")", "");
                    if (invokedButNotRunningCount == 3)
                        steplog(sb.toString(), "");
                } else {
                    steplog("Invoked", "");
                    // System.err.println(sb.toString());
                    Process p = Runtime.getRuntime().exec(cmd3);
                    workflow.addBgdProcess(p);
                    isInvoked = true;
                }
            }
            return 1;
        }
        return 0;
    }

    private void steplog(String col1, String col2) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (Formatter formatter = new Formatter(sb)) {
          String u = getUndoing() ? "U " : "";
          formatter.format(u + "%1$-8s %2$-10s %3$s", col1, col2, getFullName());
        }
        workflowGraph.getWorkflow().log(sb.toString());
    }

    @Override
    WorkflowStep newStep() {
        return new RunnableWorkflowStep();
    }

}
