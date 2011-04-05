/*
 * MyMouseMenus.java
 *
 * Created on March 21, 2007, 3:34 PM; Updated May 29, 2007
 *
 * Copyright March 21, 2007 Grotto Networking
 *
 */

package org.gusdb.workflow.visualization;

import edu.uci.ics.jung.visualization.VisualizationViewer;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.geom.Point2D;
import javax.swing.JCheckBoxMenuItem;
import javax.swing.JFrame;
import javax.swing.JMenuItem;
import javax.swing.JPopupMenu;

import org.gusdb.workflow.WorkflowStep;

public class WorkflowStepMenu extends JPopupMenu {        
    // private JFrame frame; 
    public WorkflowStepMenu(JFrame frame) {
	super("Edge Menu");
	this.add(new WorkflowStepDetailsItem(frame));           
    }    
    
    public static class WorkflowStepDetailsItem extends JMenuItem implements VertexMenuListener<WorkflowStep>,
									     MenuPointListener {
        WorkflowStep step;
        VisualizationViewer visComp;
        Point2D point;
        
        public void setVertexAndView(WorkflowStep step, VisualizationViewer visComp) {
            this.step = step;
            this.visComp = visComp;
        }
	
        public void setPoint(Point2D point) {
            this.point = point;
        }
        
        public  WorkflowStepDetailsItem(final JFrame frame) {            
            super("View Step Properties");
            this.addActionListener(new ActionListener() {
		    public void actionPerformed(ActionEvent e) {
			WorkflowStepPropertyDialog dialog = new WorkflowStepPropertyDialog(frame, step);
			dialog.setLocation((int)point.getX()+ frame.getX(), (int)point.getY()+ frame.getY());
			dialog.setVisible(true);
		    }
		    
		});
        }
        
    }
}
