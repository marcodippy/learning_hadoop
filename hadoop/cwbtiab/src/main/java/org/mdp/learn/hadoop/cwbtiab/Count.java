package org.mdp.learn.hadoop.cwbtiab;

public class Count {
  private String  product;
  private Integer count = 0;

  public Count(String product, Integer count) {
    this.product = product;
    this.count = count;
  }

  public Count(Count c) {
    this.product = c.getProduct();
    this.count = c.getCount();
  }

  public String getProduct() {
    return product;
  }

  public void setProduct(String product) {
    this.product = product;
  }

  public Integer getCount() {
    return count;
  }

  public void setCount(Integer count) {
    this.count = count;
  }

  @Override
  public String toString() {
    return "(" + product + "," + count + ")";
  }

}
