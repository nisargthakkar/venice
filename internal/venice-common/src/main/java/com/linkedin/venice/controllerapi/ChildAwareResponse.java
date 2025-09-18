package com.linkedin.venice.controllerapi;

import java.util.Map;


public class ChildAwareResponse extends ControllerResponse {
  private Map<String, String> childDataCenterControllerUrlMap;

  public Map<String, String> getChildDataCenterControllerUrlMap() {
    return childDataCenterControllerUrlMap;
  }

  public void setChildDataCenterControllerUrlMap(Map<String, String> childDataCenterControllerUrlMap) {
    this.childDataCenterControllerUrlMap = childDataCenterControllerUrlMap;
  }

  @Override
  public String toString() {
    if (childDataCenterControllerUrlMap == null) {
      return super.toString();
    } else {
      return ChildAwareResponse.class.getSimpleName() + "(childDataCenterControllerUrlMap: "
          + childDataCenterControllerUrlMap + ", super: " + super.toString() + ")";
    }
  }
}
