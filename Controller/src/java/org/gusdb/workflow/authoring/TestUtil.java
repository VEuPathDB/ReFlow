package org.gusdb.workflow.authoring;

import org.gusdb.workflow.Utilities;

public class TestUtil {

  public static void main(String[] args) {
    
    String[] words = {
        "Here I come to save the day!  And Again!",
        "Or what",
        "Oh yes, whatever you say",
        "Supercalifragilistic expialodocious"
    };
    
    int[] widths = { 3, 7, 12, 15 };
    
    for (String word : words) {
      System.out.println("*********************\n" + word);
      for (int width : widths) {
        System.out.println(width + ": " + Utilities.multiLineFormat(word, width));
      }
    }
  }
}
