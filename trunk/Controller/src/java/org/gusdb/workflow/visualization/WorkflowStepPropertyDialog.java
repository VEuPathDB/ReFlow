package org.gusdb.workflow.visualization;

import javax.swing.JLabel;
import javax.swing.JPanel;
import java.awt.Dimension;
import java.awt.Frame;
import java.util.Map;

import javax.swing.BoxLayout;
import javax.swing.JDialog;

import org.gusdb.workflow.WorkflowStep;

public class WorkflowStepPropertyDialog extends JDialog {

    private static final long serialVersionUID = 1L;
    private JPanel jContentPane = null;
    private WorkflowStep step;

    /**
     * @param owner
     */
    public WorkflowStepPropertyDialog(Frame owner, WorkflowStep step) {
        super(owner, true);
        this.step = step;
        initialize();
        setTitle("Step: " + step.getBaseName());
    }

    /**
     * This method initializes this
     * 
     * @return void
     */
    private void initialize() {
        this.setContentPane(getJContentPane());
    }

    /**
     * This method initializes jContentPane
     * 
     * @return javax.swing.JPanel
     */
    private JPanel getJContentPane() {
        if (jContentPane == null) {
	    int width = 200;
	    int height = 0;
            jContentPane = new JPanel();
            jContentPane.setLayout(new BoxLayout(jContentPane, BoxLayout.Y_AXIS));
            Map<String, String> params = step.getParamValues();
            for (String param : params.keySet()) {
		String paramDisplay = param + ": " + params.get(param);
		if ((14 * paramDisplay.length()) > width) {
		    width = (14 * paramDisplay.length());
		}
		height += 16;
		jContentPane.add(new JLabel(paramDisplay));
            }
	    setSize(new Dimension(width, height));
        }
        return jContentPane;
    }

}
