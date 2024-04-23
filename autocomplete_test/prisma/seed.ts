// prisma/vehicles.ts

import { Vehicle } from "@prisma/client";
import * as createCsvWriter from "csv-writer";
import * as fs from "fs";
const csv = require("csv-parser"); // Using require syntax here

import path from "path";

import { PrismaClient } from '@prisma/client'
const prisma = new PrismaClient()

const problematicRecords: Array<{
  year: number;
  make: string;
  model: string;
}> = [];

export default async function main(
  batchSize = 25000,
) {
  const vehicles: Array<{
    year: string;
    make: string;
    model: string;
    type: string;
  }> = [];
  const returnedVehicles: Vehicle[] = [];
  return new Promise((resolve, reject) => {

    console.log("Seeding vehicles data...");

    fs.createReadStream(path.dirname(__filename) + "/vehicles.csv")
      .pipe(csv())
      .on(
        "data",
        (data: { year: string; make: string; model: string; type: string }) =>
          vehicles.push(data),
      )
      .on("end", async () => {
        try {
          for (let i = 0; i < vehicles.length; i += batchSize) {
            if (!vehicles)
              throw new Error("No vehicle data found in vehicles.csv");

            let batch = vehicles.slice(i, i + batchSize).map((vehicle) => {

              if (vehicle.year === "0" || vehicle.year === "0000") {
                console.warn(
                  `Vehicle year "${vehicle.year}" is not valid. Skipping...`,
                );
                return null;
              }
              if (vehicle.make.length === 0) {
                console.warn(
                  `Vehicle make "${vehicle.make}" is not valid. Skipping...`,
                );

                return null;
              }
              if (vehicle.model.length === 0) {
                console.warn(
                  `Vehicle model "${vehicle.model}" is not valid. Skipping...`,
                );
                return null;
              }
              return {
                year: parseInt(vehicle.year, 10),
                make: vehicle.make?.slice(0, 50),
                model: vehicle.model?.slice(0, 128),
              };
            });

            try {
              // Filtering out null values properly
              const filteredBatch: Array<{ year: number; make: string; model: string }> = batch.filter((vehicle): vehicle is { year: number; make: string; model: string } => vehicle !== null);

              await prisma.vehicle.createMany({ data: filteredBatch });
              console.log(`Inserted ${i + filteredBatch.length} records so far...`);
            } catch (error: any) {
              console.error(
                "Error inserting batch starting at index:",
                i,
                "\nError:",
                error.message,
              );
              problematicRecords.push(...batch.filter((v): v is { year: number; make: string; model: string } => v !== null));
            }
          }

          console.log("Data seeding process completed!");

          if (problematicRecords.length > 0) {
            const csvWriter = createCsvWriter.createObjectCsvWriter({
              path: "./prisma/problematic_vehicles.csv",
              header: [
                { id: "year", title: "YEAR" },
                { id: "make", title: "MAKE" },
                { id: "model", title: "MODEL" },
              ],
            });

            await csvWriter.writeRecords(problematicRecords);
            console.log(
              "Problematic records saved to problematic_vehicles.csv",
            );
          }

          // Fetch and return the first 10 vehicles
          returnedVehicles.push(
            ...(await prisma.vehicle.findMany({
              take: 50,
            })),
          );
          resolve(returnedVehicles);
        } catch (error: any) {
          console.error("An error occurred during processing:", error.message);
          reject(error);
        } finally {
          await prisma.$disconnect();
        }
      });
  });
}
