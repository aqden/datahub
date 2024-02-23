describe("edit dataset administration", () => {
  it("open test dataset page, edit documentation", () => {
    //edit documentation and verify changes saved
    cy.loginWithCredentials();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)/Schema"
    );
    cy.openEntityTab("Dataset Administration");
    // check the various options
    cy.waitTextVisible("Soft Delete Dataset");
    cy.waitTextVisible("Edit Browsepath");
    cy.waitTextVisible("Edit Dataset Container");
    cy.waitTextVisible("Edit Dataset Display Name");

  });
});