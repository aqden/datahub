describe("edit schema", () => {
  it("open test dataset page, edit documentation", () => {
    //edit documentation and verify changes saved
    cy.loginWithCredentials();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)/Schema"
    );
    cy.openEntityTab("Edit Schema");
    cy.get("[data-testid='edit-schema-button']").should("be.visible");
    cy.get("[data-testid='delete-schema-button']").should("be.visible");
  });
});