describe("edit properties", () => {
  it("open test dataset page, edit documentation", () => {
    //edit documentation and verify changes saved
    cy.loginWithCredentials();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)/Schema"
    );
    cy.openEntityTab("Edit Properties");
    cy.get("[data-testid='edit-properties-button']").should("be.visible");
    cy.get("[data-testid='delete-properties-button']").should("be.visible");
  });
});